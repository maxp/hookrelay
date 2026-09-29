-- activate_retry_v1: make a Recipient head whose retry is due claimable
-- again. Cooperative maintenance calls it for members of hr1:retries; the
-- member is only a locator, so the script re-validates the authoritative
-- head state and Valkey TIME before mutating. Per-recipient keys are
-- resolved inside the script (standalone Valkey is a precondition).
--
-- KEYS[1] ready index     hr1:ready
-- KEYS[2] ready sequence  hr1:ready_seq
-- KEYS[3] retry index     hr1:retries
-- KEYS[4] blocked index   hr1:blocked
--
-- ARGV[1] recipient_identity
-- ARGV[2] key prefix ("hr1")
--
-- Preconditions in order: the retry index has the wrong type (wrong_type;
-- the marker path writes it); block marker present (recipient_blocked; the
-- stale retries member is removed); expected key types and a well-formed
-- ready sequence (wrong_type); head state not retry_wait (not_due; the
-- stale retries member is removed); malformed attempt or retry_at_ms, or a
-- head_message_id that is not the queue head (wrong_type: corrupt state is
-- left for reconciliation, never activated); retry_at_ms later than TIME
-- (not_due; nothing changes).
--
-- Writes: head state -> ready, keeping delivery_cycle and attempt (the
-- attempt to be claimed next) and clearing retry_at_ms; retries member
-- removed; ready member added with a fresh sequence.
--
-- Returns:
--   {"activated", message_id, attempt}
--   {"not_due"}
--   {"recipient_blocked"}
--   {"wrong_type"}

if #KEYS ~= 4 or #ARGV ~= 2 then
  return redis.error_reply('ERR activate_retry_v1: expected 4 keys and 2 arguments')
end
for i = 1, 2 do
  if ARGV[i] == '' then
    return redis.error_reply('ERR activate_retry_v1: argument ' .. i .. ' is empty')
  end
end
local rid, prefix = ARGV[1], ARGV[2]
if KEYS[1] ~= prefix .. ':ready' or KEYS[2] ~= prefix .. ':ready_seq' or KEYS[3] ~= prefix .. ':retries'
    or KEYS[4] ~= prefix .. ':blocked' then
  return redis.error_reply('ERR activate_retry_v1: keys do not match the prefix')
end

local readyKey, readySeqKey, retriesKey, blockedKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
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

-- 1. A blocked Recipient is never activated; its retries member is stale.
if not has_type(retriesKey, 'zset') then
  return {'wrong_type'}
end
if type_of(markerKey) ~= 'none' then
  redis.call('ZREM', retriesKey, rid)
  return {'recipient_blocked'}
end

-- 2. Key types and encodings.
if not (has_type(queueKey, 'list') and has_type(stateKey, 'hash') and has_type(readyKey, 'zset')
    and has_type(readySeqKey, 'string') and has_type(blockedKey, 'zset')) then
  return {'wrong_type'}
end
local seq = redis.call('GET', readySeqKey)
if seq and (not (seq == '0' or string.match(seq, '^[1-9]%d*$')) or #seq > 19
    or (#seq == 19 and seq >= '9223372036854775807')) then
  return {'wrong_type'}
end

-- 3. Only a due retry_wait head is activated.
local state = redis.call('HMGET', stateKey, 'status', 'head_message_id', 'attempt', 'retry_at_ms')
if state[1] ~= 'retry_wait' then
  redis.call('ZREM', retriesKey, rid)
  return {'not_due'}
end
if not (state[4] and string.match(state[4], '^[1-9]%d*$') and #state[4] <= 15)
    or not (state[3] and string.match(state[3], '^[1-9]%d*$') and #state[3] <= 15) or not state[2]
    or redis.call('LINDEX', queueKey, 0) ~= state[2] then
  return {'wrong_type'}
end
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
if tonumber(state[4]) > now_ms then
  return {'not_due'}
end

-- Writes.
redis.call('HSET', stateKey, 'status', 'ready')
redis.call('HDEL', stateKey, 'retry_at_ms')
redis.call('ZREM', retriesKey, rid)
local nextSeq = redis.call('INCR', readySeqKey)
redis.call('ZADD', readyKey, nextSeq, rid)

return {'activated', state[2], tonumber(state[3])}
