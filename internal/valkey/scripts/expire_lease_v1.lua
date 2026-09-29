-- expire_lease_v1: expire one Message Lease past its deadline as a failed
-- Delivery Attempt. Cooperative maintenance calls it for members of
-- hr1:leases; the member is only a locator, so the script re-validates the
-- authoritative head state and Valkey TIME before mutating. The attempt is
-- appended to the history (outcome=expired) and the head waits in
-- retry_wait exactly as after nack. Per-recipient keys are resolved inside
-- the script (standalone Valkey is a precondition).
--
-- KEYS[1] ready index     hr1:ready
-- KEYS[2] ready sequence  hr1:ready_seq
-- KEYS[3] lease index     hr1:leases
-- KEYS[4] retry index     hr1:retries
-- KEYS[5] blocked index   hr1:blocked
-- KEYS[6] DLQ index       hr1:dlq
-- KEYS[7] queued counter  hr1:stats:queued_messages
--
-- ARGV[1] recipient_identity
-- ARGV[2] retry_delays_ms: comma-separated positive integers, entry n is
--         the drawn effective delay after failed attempt n; exactly
--         max_attempts - 1 entries (empty when max_attempts is 1)
-- ARGV[3] max_attempts
-- ARGV[4] tombstone_ttl_ms (1 hour)
-- ARGV[5] key prefix ("hr1")
--
-- Preconditions in order: the lease index has the wrong type (wrong_type;
-- the marker path writes it); block marker present (recipient_blocked; the
-- stale lease member is removed); queue or head state of the wrong type
-- (wrong_type); head state not leased (not_due; the stale lease member is
-- removed); the other key types (wrong_type); lease not yet due (not_due;
-- nothing changes); malformed state fields, a
-- head_message_id that is not the queue head, a malformed history entry,
-- or a token record of the wrong type (wrong_type); the expired attempt is
-- below max_attempts (attempts_exhausted: refused without mutation until
-- the dead-letter transition exists).
--
-- The token record is resolved through the state's delivery_token_digest
-- (written by claim_v3) and, when it still belongs to this message, moves
-- to the expired phase with a tombstone TTL; its claim operation record
-- becomes no_longer_active. A lease
-- claimed before the digest existed still expires; its token and operation
-- records are left to their TTLs (ack, nack, and claim replay already see
-- a head that is no longer leased as stale).
--
-- History entries and the 10-cycle archive fold follow nack_v1.
--
-- Returns:
--   {"retry_scheduled", message_id, attempt, retry_at_ms, delivery_cycle, claimed_ms, expired_ms, consumer_instance_id}
--   {"not_due"}
--   {"recipient_blocked"}
--   {"attempts_exhausted"}
--   {"wrong_type"}

if #KEYS ~= 7 or #ARGV ~= 5 then
  return redis.error_reply('ERR expire_lease_v1: expected 7 keys and 5 arguments')
end
for _, i in ipairs({1, 3, 4, 5}) do
  if ARGV[i] == '' then
    return redis.error_reply('ERR expire_lease_v1: argument ' .. i .. ' is empty')
  end
end
for _, i in ipairs({3, 4}) do
  if not string.match(ARGV[i], '^[1-9][0-9]*$') or #ARGV[i] > 15 then
    return redis.error_reply('ERR expire_lease_v1: argument ' .. i .. ' must be a positive integer')
  end
end
local maxAttempts = tonumber(ARGV[3])
local delays = {}
for d in string.gmatch(ARGV[2] ~= '' and ARGV[2] .. ',' or '', '([^,]*),') do
  if not string.match(d, '^[1-9][0-9]*$') or #d > 15 then
    return redis.error_reply('ERR expire_lease_v1: retry_delays_ms must be positive integers')
  end
  delays[#delays + 1] = tonumber(d)
end
if #delays ~= maxAttempts - 1 then
  return redis.error_reply('ERR expire_lease_v1: retry_delays_ms must have max_attempts - 1 entries')
end
local rid, prefix = ARGV[1], ARGV[5]
if KEYS[1] ~= prefix .. ':ready' or KEYS[2] ~= prefix .. ':ready_seq' or KEYS[3] ~= prefix .. ':leases'
    or KEYS[4] ~= prefix .. ':retries' or KEYS[5] ~= prefix .. ':blocked' or KEYS[6] ~= prefix .. ':dlq'
    or KEYS[7] ~= prefix .. ':stats:queued_messages' then
  return redis.error_reply('ERR expire_lease_v1: keys do not match the prefix')
end

local readyKey, readySeqKey, leasesKey, retriesKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local blockedKey, dlqKey, counterKey = KEYS[5], KEYS[6], KEYS[7]
local queueKey = prefix .. ':r:' .. rid .. ':q'
local stateKey = prefix .. ':r:' .. rid .. ':s'
local markerKey = prefix .. ':q:' .. rid

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, expected)
  local t = type_of(key)
  return t == 'none' or t == expected
end
local function valid_ms(value)
  return value and string.match(value, '^[1-9]%d*$') and #value <= 15
end

-- 1. A blocked Recipient is never expired; its lease member is stale.
if not has_type(leasesKey, 'zset') then
  return {'wrong_type'}
end
if type_of(markerKey) ~= 'none' then
  redis.call('ZREM', leasesKey, rid)
  return {'recipient_blocked'}
end

-- 2. Only a leased head is expired; any other member is stale.
if not (has_type(queueKey, 'list') and has_type(stateKey, 'hash')) then
  return {'wrong_type'}
end
local state = redis.call('HMGET', stateKey, 'status', 'head_message_id', 'lease_expires_ms', 'delivery_cycle',
  'attempt', 'claimed_ms', 'consumer_instance_id', 'delivery_token_digest')
if state[1] ~= 'leased' then
  redis.call('ZREM', leasesKey, rid)
  return {'not_due'}
end

-- 3. The remaining key types, then the deadline.
if not (has_type(readyKey, 'zset') and has_type(readySeqKey, 'string') and has_type(retriesKey, 'zset')
    and has_type(blockedKey, 'zset') and has_type(dlqKey, 'zset') and has_type(counterKey, 'string')) then
  return {'wrong_type'}
end
if not (valid_ms(state[3]) and valid_ms(state[4]) and valid_ms(state[5]) and valid_ms(state[6])) then
  return {'wrong_type'}
end
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
if tonumber(state[3]) > now_ms then
  return {'not_due'}
end
local messageID = state[2]
if not messageID or redis.call('LINDEX', queueKey, 0) ~= messageID then
  return {'wrong_type'}
end
local leaseExpires, cycle, attempt, claimedMs = tonumber(state[3]), tonumber(state[4]), tonumber(state[5]), tonumber(state[6])
local historyKey = prefix .. ':a:' .. messageID
if not has_type(historyKey, 'list') then
  return {'wrong_type'}
end

-- 4. Token record through the state's digest (absent for claim_v2 leases).
local tokenKey, opKey = nil, nil
if state[8] and state[8] ~= '' then
  tokenKey = prefix .. ':t:' .. state[8]
  if not has_type(tokenKey, 'hash') then
    return {'wrong_type'}
  end
  local tok = redis.call('HMGET', tokenKey, 'state', 'message_id', 'operation_id')
  if tok[1] and tok[2] ~= messageID then
    tokenKey = nil -- a record for another message is never overwritten
  end
  local operationID = tokenKey and tok[1] == 'active' and tok[3]
  if operationID then
    opKey = prefix .. ':op:' .. operationID
    if not has_type(opKey, 'hash') then
      return {'wrong_type'}
    end
  end
end

-- 5. Parse the existing history before any write.
local function integer(v)
  return type(v) == 'number' and v >= 0 and v == math.floor(v)
end
local entries = {}
for i, raw in ipairs(redis.call('LRANGE', historyKey, 0, -1)) do
  local ok, e = pcall(cjson.decode, raw)
  if not ok or type(e) ~= 'table' then
    return {'wrong_type'}
  end
  if e['kind'] == 'attempt' then
    if not (integer(e['delivery_cycle']) and integer(e['attempt']) and integer(e['claimed_ms'])
        and integer(e['lease_expires_ms']) and integer(e['completed_ms'])) then
      return {'wrong_type'}
    end
  elseif e['kind'] == 'archived_cycles_summary' and i == 1 then
    if not (integer(e['archived_cycles']) and integer(e['archived_attempts'])
        and integer(e['first_archived_ms']) and integer(e['last_archived_ms'])) then
      return {'wrong_type'}
    end
  else
    return {'wrong_type'}
  end
  entries[#entries + 1] = {raw = raw, e = e}
end

-- 6. The last attempt needs the dead-letter transition.
if attempt >= maxAttempts then
  return {'attempts_exhausted'}
end

-- Writes.
local retryAt = now_ms + delays[attempt]
local entry = string.format(
  '{"kind":"attempt","delivery_cycle":%d,"attempt":%d,"claimed_ms":%d,"lease_expires_ms":%d,"completed_ms":%d,"outcome":"expired"',
  cycle, attempt, claimedMs, leaseExpires, now_ms)
local instance = state[7]
if instance and #instance <= 64 and string.match(instance, '^[%w_.:%-]+$') then
  entry = entry .. ',"consumer_instance_id":"' .. instance .. '"'
end
entry = entry .. '}'
entries[#entries + 1] = {raw = entry, e = {kind = 'attempt', delivery_cycle = cycle, claimed_ms = claimedMs, completed_ms = now_ms}}

-- Fold the oldest cycles beyond the latest 10 into the leading summary.
local cycles, seen = {}, {}
for _, x in ipairs(entries) do
  local c = x.e['delivery_cycle']
  if x.e['kind'] == 'attempt' and not seen[c] then
    seen[c] = true
    cycles[#cycles + 1] = c
  end
end
if #cycles > 10 then
  table.sort(cycles)
  local archived = {}
  for i = 1, #cycles - 10 do
    archived[cycles[i]] = true
  end
  local sum = {cycles = 0, attempts = 0}
  local kept = {}
  for _, x in ipairs(entries) do
    local e = x.e
    if e['kind'] == 'archived_cycles_summary' then
      sum.cycles, sum.attempts = e['archived_cycles'], e['archived_attempts']
      sum.first, sum.last = e['first_archived_ms'], e['last_archived_ms']
    elseif archived[e['delivery_cycle']] then
      sum.attempts = sum.attempts + 1
      if not sum.first or e['claimed_ms'] < sum.first then
        sum.first = e['claimed_ms']
      end
      if not sum.last or e['completed_ms'] > sum.last then
        sum.last = e['completed_ms']
      end
    else
      kept[#kept + 1] = x.raw
    end
  end
  sum.cycles = sum.cycles + (#cycles - 10)
  redis.call('DEL', historyKey)
  redis.call('RPUSH', historyKey, string.format(
    '{"kind":"archived_cycles_summary","archived_cycles":%d,"archived_attempts":%d,"first_archived_ms":%d,"last_archived_ms":%d}',
    sum.cycles, sum.attempts, sum.first, sum.last))
  redis.call('RPUSH', historyKey, unpack(kept))
else
  redis.call('RPUSH', historyKey, entry)
end

-- The head waits for its retry; no lease fields or plaintext token remain.
redis.call('DEL', stateKey)
redis.call('HSET', stateKey,
  'status', 'retry_wait',
  'head_message_id', messageID,
  'delivery_cycle', cycle,
  'attempt', attempt + 1,
  'retry_at_ms', retryAt)
redis.call('ZREM', leasesKey, rid)
redis.call('ZREM', readyKey, rid)
redis.call('ZADD', retriesKey, retryAt, rid)

-- Terminal token phase and the claim operation marker.
if tokenKey then
  redis.call('DEL', tokenKey)
  redis.call('HSET', tokenKey, 'state', 'expired', 'message_id', messageID, 'expired_ms', now_ms)
  redis.call('PEXPIRE', tokenKey, tonumber(ARGV[4]))
end
if opKey then
  local opTTL = redis.call('PTTL', opKey)
  if opTTL > 0 then
    local argsDigest = redis.call('HGET', opKey, 'args_digest')
    redis.call('DEL', opKey)
    redis.call('HSET', opKey, 'kind', 'claim', 'args_digest', argsDigest or '', 'state', 'no_longer_active')
    redis.call('PEXPIRE', opKey, opTTL)
  end
end

return {'retry_scheduled', messageID, attempt, retryAt, cycle, claimedMs, now_ms, state[7] or ''}
