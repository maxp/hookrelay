-- claim_v1: claim the next ready Recipient head for a consumer, or replay a
-- recorded claim operation. The script never waits; long polling repeats it
-- process-side. Per-recipient keys are resolved inside the script from the
-- ready-index member (standalone Valkey is a precondition; Cluster would need
-- declared keys). Valkey TIME is authoritative for claimed_ms and the lease
-- deadline.
--
-- KEYS[1] ready index    hr1:ready
-- KEYS[2] lease index    hr1:leases
-- KEYS[3] blocked index  hr1:blocked
--
-- ARGV[1]  operation_id
-- ARGV[2]  args_digest (lowercase hex SHA-256 over operation_id and wait_ms)
-- ARGV[3]  max_active_leases
-- ARGV[4]  claim_op_ttl_ms
-- ARGV[5]  initial_lease_duration_ms
-- ARGV[6]  delivery_token (dlv_<base64url-128-bit>, app-generated)
-- ARGV[7]  delivery_token_digest (lowercase hex SHA-256 of the full token)
-- ARGV[8]  consumer_instance_id (empty when absent)
-- ARGV[9]  record_empty ("1" records an empty outcome, "0" does not; a
--          waiting claim records only its final empty check)
-- ARGV[10] key prefix ("hr1")
--
-- Flow: (1) operation replay; (2) active-lease limit over unexpired leases;
-- (3) scan at most 10 lowest-scored ready candidates, blocking inconsistent
-- ones and skipping stale derived entries; (4) claim the first valid head.
--
-- Returns (claimed and replay_active share one shape):
--   {"claimed", token, message_id, delivery_cycle, attempt, claimed_ms, lease_expires_ms, message_json, blocked_detected}
--   {"replay_active", token, message_id, delivery_cycle, attempt, claimed_ms, lease_expires_ms, message_json, 0}
--   {"replay_empty"}
--   {"claim_no_longer_active"}
--   {"operation_conflict"}
--   {"limit_exceeded"}
--   {"empty", blocked_detected}
--   {"wrong_type"}

if #KEYS ~= 3 or #ARGV ~= 10 then
  return redis.error_reply('ERR claim_v1: expected 3 keys and 10 arguments')
end
for _, i in ipairs({1, 2, 3, 4, 5, 6, 7, 10}) do
  if ARGV[i] == '' then
    return redis.error_reply('ERR claim_v1: argument ' .. i .. ' is empty')
  end
end
for _, i in ipairs({3, 4, 5}) do
  if not string.match(ARGV[i], '^[1-9][0-9]*$') then
    return redis.error_reply('ERR claim_v1: argument ' .. i .. ' must be a positive integer')
  end
end
if ARGV[9] ~= '0' and ARGV[9] ~= '1' then
  return redis.error_reply('ERR claim_v1: record_empty must be 0 or 1')
end
local prefix = ARGV[10]
if KEYS[1] ~= prefix .. ':ready' or KEYS[2] ~= prefix .. ':leases' or KEYS[3] ~= prefix .. ':blocked' then
  return redis.error_reply('ERR claim_v1: keys do not match the prefix')
end

local readyKey, leasesKey, blockedKey = KEYS[1], KEYS[2], KEYS[3]
local opKey = prefix .. ':op:' .. ARGV[1]

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, expected)
  local t = type_of(key)
  return t == 'none' or t == expected
end

if not (has_type(readyKey, 'zset') and has_type(leasesKey, 'zset') and has_type(blockedKey, 'zset') and has_type(opKey, 'hash')) then
  return {'wrong_type'}
end

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

-- 1. Operation replay.
if redis.call('EXISTS', opKey) == 1 then
  local op = redis.call('HMGET', opKey, 'kind', 'args_digest', 'state', 'delivery_token', 'message_id',
    'recipient_identity', 'delivery_cycle', 'attempt', 'claimed_ms', 'lease_expires_ms')
  if op[1] ~= 'claim' or op[2] ~= ARGV[2] then
    return {'operation_conflict'}
  end
  if op[3] == 'empty' then
    return {'replay_empty'}
  end
  if op[3] ~= 'active' then
    return {'claim_no_longer_active'}
  end
  for i = 4, 10 do
    if not op[i] then
      return {'claim_no_longer_active'}
    end
  end
  -- The recorded attempt must still be the current leased head.
  local stateKey = prefix .. ':r:' .. op[6] .. ':s'
  if not has_type(stateKey, 'hash') then
    return {'claim_no_longer_active'}
  end
  local state = redis.call('HMGET', stateKey, 'status', 'delivery_token', 'lease_expires_ms')
  local blob = redis.call('GET', prefix .. ':m:' .. op[5])
  if state[1] ~= 'leased' or state[2] ~= op[4] or not blob or (tonumber(state[3]) or 0) <= now_ms then
    return {'claim_no_longer_active'}
  end
  return {'replay_active', op[4], op[5], op[7], op[8], op[9], op[10], blob, 0}
end

-- 2. Work-pool-wide active-lease limit over unexpired leases.
if redis.call('ZCOUNT', leasesKey, '(' .. now_ms, '+inf') >= tonumber(ARGV[3]) then
  return {'limit_exceeded'}
end

-- 3. Candidate scan.
local blocked = 0
local function block(rid, reason)
  local markerKey = prefix .. ':q:' .. rid
  local detected = now_ms
  local mt = type_of(markerKey)
  if mt == 'none' then
    redis.call('HSET', markerKey, 'detected_ms', now_ms, 'reason_code', reason)
    blocked = blocked + 1
  elseif mt == 'hash' then
    -- An existing marker keeps its reason and detection time.
    detected = tonumber(redis.call('HGET', markerKey, 'detected_ms')) or now_ms
  end
  redis.call('ZADD', blockedKey, detected, rid)
  redis.call('ZREM', readyKey, rid)
  redis.call('ZREM', leasesKey, rid)
end

local candidates = redis.call('ZRANGE', readyKey, 0, 9)
for _, rid in ipairs(candidates) do
  local queueKey = prefix .. ':r:' .. rid .. ':q'
  local stateKey = prefix .. ':r:' .. rid .. ':s'
  local markerKey = prefix .. ':q:' .. rid
  local qt, st = type_of(queueKey), type_of(stateKey)
  if type_of(markerKey) ~= 'none' then
    block(rid, 'unsupported_key_type') -- keeps an existing marker's reason
  elseif (qt ~= 'none' and qt ~= 'list') or (st ~= 'none' and st ~= 'hash') then
    block(rid, 'unsupported_key_type')
  elseif qt == 'none' and st == 'none' then
    -- Stale derived entry for a drained queue: safe to drop.
    redis.call('ZREM', readyKey, rid)
  elseif st == 'none' then
    block(rid, 'head_state_missing')
  else
    local state = redis.call('HMGET', stateKey, 'status', 'head_message_id', 'delivery_cycle', 'attempt')
    local head = redis.call('LINDEX', queueKey, 0)
    if state[1] == 'leased' and head and state[2] == head then
      -- A leased head is not ready: drop the stale derived entry.
      redis.call('ZREM', readyKey, rid)
    elseif state[1] ~= 'ready' or not head or state[2] ~= head then
      block(rid, 'queue_head_mismatch')
    elseif not (state[3] and state[4]) then
      block(rid, 'head_state_missing')
    else
      local blobKey = prefix .. ':m:' .. head
      local blob = type_of(blobKey) == 'string' and redis.call('GET', blobKey) or nil
      if not blob then
        block(rid, 'head_message_missing')
      else
        -- 4. Claim.
        local ttl = tonumber(ARGV[4])
        local lease_ms = now_ms + tonumber(ARGV[5])
        redis.call('HSET', stateKey,
          'status', 'leased',
          'delivery_token', ARGV[6],
          'claimed_ms', now_ms,
          'lease_expires_ms', lease_ms)
        if ARGV[8] ~= '' then
          redis.call('HSET', stateKey, 'consumer_instance_id', ARGV[8])
        else
          redis.call('HDEL', stateKey, 'consumer_instance_id')
        end
        redis.call('ZADD', leasesKey, lease_ms, rid)
        redis.call('ZREM', readyKey, rid)
        redis.call('DEL', opKey)
        redis.call('HSET', opKey,
          'kind', 'claim',
          'args_digest', ARGV[2],
          'state', 'active',
          'delivery_token', ARGV[6],
          'message_id', head,
          'recipient_identity', rid,
          'delivery_cycle', state[3],
          'attempt', state[4],
          'claimed_ms', now_ms,
          'lease_expires_ms', lease_ms)
        redis.call('PEXPIRE', opKey, ttl)
        local tokenKey = prefix .. ':t:' .. ARGV[7]
        redis.call('DEL', tokenKey)
        redis.call('HSET', tokenKey,
          'state', 'active',
          'recipient_identity', rid,
          'message_id', head,
          'operation_id', ARGV[1],
          'claimed_ms', now_ms,
          'lease_expires_ms', lease_ms)
        redis.call('PEXPIRE', tokenKey, ttl + tonumber(ARGV[5]))
        return {'claimed', ARGV[6], head, state[3], state[4], now_ms, lease_ms, blob, blocked}
      end
    end
  end
end

-- 5. No valid candidate.
if ARGV[9] == '1' then
  redis.call('HSET', opKey, 'kind', 'claim', 'args_digest', ARGV[2], 'state', 'empty')
  redis.call('PEXPIRE', opKey, tonumber(ARGV[4]))
end
return {'empty', blocked}
