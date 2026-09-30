-- clear_block_v2: clear one Recipient block marker after the operator's
-- exact preconditions match and every authoritative-state invariant
-- re-verifies, restoring exactly the derived index implied by the verified
-- state and appending the mandatory audit event in the same operation. It
-- never repairs authoritative state: any ambiguity is refused without
-- mutation. Per-recipient keys are resolved inside the script (standalone
-- Valkey is a precondition).
--
-- KEYS[1] ready index     hr1:ready
-- KEYS[2] ready sequence  hr1:ready_seq
-- KEYS[3] lease index     hr1:leases
-- KEYS[4] retry index     hr1:retries
-- KEYS[5] blocked index   hr1:blocked
-- KEYS[6] audit stream    hr1:audit
--
-- ARGV[1] recipient_identity
-- ARGV[2] expected_detected_ms
-- ARGV[3] expected_reason_code
-- ARGV[4] event_id
-- ARGV[5] request_id
-- ARGV[6] message check bound (per-recipient queue limit)
-- ARGV[7] key prefix ("hr1")
--
-- Preconditions in order: global key types and the ready sequence
-- (wrong_type); marker absent (not_found) or not a Hash (wrong_type);
-- detected_ms/reason_code differ from the expected values
-- (precondition_failed); the invariants of reconcile_recipient_v3, reported
-- as the first violated one (ambiguous): unsupported_key_type,
-- queue_head_mismatch, head_state_missing, head_message_missing,
-- queued_delivery_state_missing, queued_delivery_state_invalid.
-- clear_block_v1 plus the queued pending-state invariants, so a block
-- caused by damaged replay metadata cannot be cleared while the damage
-- remains, even when the head itself is healthy.
--
-- Writes: DEL marker, ZREM blocked, restore the membership implied by the
-- verified state (ready with a fresh sequence, leases at lease_expires_ms,
-- retries at retry_at_ms, or none for an empty queue) and remove the
-- others, XADD audit operation=recipient_block_cleared.
--
-- Returns:
--   {"cleared", restored index: ready | leases | retries | none}
--   {"not_found"}
--   {"precondition_failed"}
--   {"ambiguous", invariant}
--   {"wrong_type"}

if #KEYS ~= 6 or #ARGV ~= 7 then
  return redis.error_reply('ERR clear_block_v2: expected 6 keys and 7 arguments')
end
for _, i in ipairs({1, 3, 4, 5, 7}) do
  if ARGV[i] == '' then
    return redis.error_reply('ERR clear_block_v2: argument ' .. i .. ' is empty')
  end
end
for _, i in ipairs({2, 6}) do
  if not string.match(ARGV[i], '^[1-9][0-9]*$') or #ARGV[i] > 15 then
    return redis.error_reply('ERR clear_block_v2: argument ' .. i .. ' must be a positive integer')
  end
end
local rid, prefix = ARGV[1], ARGV[7]
if KEYS[1] ~= prefix .. ':ready' or KEYS[2] ~= prefix .. ':ready_seq' or KEYS[3] ~= prefix .. ':leases'
    or KEYS[4] ~= prefix .. ':retries' or KEYS[5] ~= prefix .. ':blocked' or KEYS[6] ~= prefix .. ':audit' then
  return redis.error_reply('ERR clear_block_v2: keys do not match the prefix')
end
local readyKey, readySeqKey, leasesKey, retriesKey, blockedKey, auditKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
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

-- Saved delivery state of a queued non-head message: nil when valid (a
-- complete pending pair, or neither pending field and no attempt history),
-- else the violated invariant. The attempt is never reset.
local function queued_state_issue(id)
  local metaKey = prefix .. ':mi:' .. id
  local mt = type_of(metaKey)
  local p = {false, false}
  if mt == 'hash' then
    p = redis.call('HMGET', metaKey, 'pending_delivery_cycle', 'pending_attempt')
  elseif mt ~= 'none' then
    return 'queued_delivery_state_invalid'
  end
  if p[1] or p[2] then
    if valid_ms(p[1]) and valid_ms(p[2]) then
      return nil
    end
    return 'queued_delivery_state_invalid'
  end
  if type_of(prefix .. ':a:' .. id) ~= 'none' then
    return 'queued_delivery_state_missing'
  end
  return nil
end

-- 1. Global structures.
if not (has_type(readyKey, 'zset') and has_type(leasesKey, 'zset') and has_type(retriesKey, 'zset')
    and has_type(blockedKey, 'zset') and has_type(readySeqKey, 'string') and has_type(auditKey, 'stream')) then
  return {'wrong_type'}
end
local seq = redis.call('GET', readySeqKey)
if seq and (not (seq == '0' or string.match(seq, '^[1-9]%d*$')) or #seq > 19
    or (#seq == 19 and seq >= '9223372036854775807')) then
  return {'wrong_type'}
end

-- 2. The marker and the operator's exact preconditions.
local mt = type_of(markerKey)
if mt == 'none' then
  return {'not_found'}
end
if mt ~= 'hash' then
  return {'wrong_type'}
end
local marker = redis.call('HMGET', markerKey, 'detected_ms', 'reason_code')
if marker[1] ~= ARGV[2] or marker[2] ~= ARGV[3] then
  return {'precondition_failed'}
end

-- 3. Authoritative-state invariants (reconcile_recipient_v2 rules).
local qt, st = type_of(queueKey), type_of(stateKey)
if (qt ~= 'none' and qt ~= 'list') or (st ~= 'none' and st ~= 'hash') then
  return {'ambiguous', 'unsupported_key_type'}
end
local restore, score = 'none', nil
if not (qt == 'none' and st == 'none') then
  if qt == 'none' then
    return {'ambiguous', 'queue_head_mismatch'}
  end
  if st == 'none' then
    return {'ambiguous', 'head_state_missing'}
  end
  local state = redis.call('HMGET', stateKey, 'status', 'head_message_id', 'delivery_cycle', 'attempt',
    'lease_expires_ms', 'delivery_token', 'retry_at_ms')
  if state[2] ~= redis.call('LINDEX', queueKey, 0) then
    return {'ambiguous', 'queue_head_mismatch'}
  end
  if not (state[3] and state[4]) or (state[1] ~= 'ready' and state[1] ~= 'leased' and state[1] ~= 'retry_wait') then
    return {'ambiguous', 'head_state_missing'}
  end
  local ids = redis.call('LRANGE', queueKey, 0, tonumber(ARGV[6]) - 1)
  for _, id in ipairs(ids) do
    if type_of(prefix .. ':m:' .. id) ~= 'string' then
      return {'ambiguous', 'head_message_missing'}
    end
  end
  for i = 2, #ids do
    local issue = queued_state_issue(ids[i])
    if issue then
      return {'ambiguous', issue}
    end
  end
  if state[1] == 'leased' then
    if not (valid_ms(state[5]) and state[6]) then
      return {'ambiguous', 'head_state_missing'}
    end
    restore, score = 'leases', state[5]
  elseif state[1] == 'retry_wait' then
    if not valid_ms(state[7]) then
      return {'ambiguous', 'head_state_missing'}
    end
    restore, score = 'retries', state[7]
  else
    restore = 'ready'
  end
end

-- Writes.
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('DEL', markerKey)
redis.call('ZREM', blockedKey, rid)
redis.call('ZREM', readyKey, rid)
redis.call('ZREM', leasesKey, rid)
redis.call('ZREM', retriesKey, rid)
if restore == 'ready' then
  redis.call('ZADD', readyKey, redis.call('INCR', readySeqKey), rid)
elseif restore == 'leases' then
  redis.call('ZADD', leasesKey, score, rid)
elseif restore == 'retries' then
  redis.call('ZADD', retriesKey, score, rid)
end
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[4],
  'timestamp_ms', now_ms,
  'actor', 'admin_bearer',
  'operation', 'recipient_block_cleared',
  'target', rid,
  'request_id', ARGV[5],
  'outcome', 'success',
  'reason', ARGV[3])

return {'cleared', restore}
