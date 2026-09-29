-- extend_v1: extend one Message Lease by its Delivery Token, idempotent by
-- operation_id. The server chooses the extension; the new deadline is
-- min(lease + extension_ms, attempt start + max_lifetime_ms), where the
-- attempt start is attempt_started_ms (claimed_ms for a lease claimed
-- before that field existed). Valkey TIME is authoritative for the
-- deadline comparison. The claim operation record keeps its recorded claim
-- response. Per-recipient keys are resolved inside the script from the
-- token record (standalone Valkey is a precondition).
--
-- KEYS[1] token record      hr1:t:<delivery_token_digest>
-- KEYS[2] operation record  hr1:op:<operation_id>
-- KEYS[3] lease index       hr1:leases
--
-- ARGV[1] delivery_token
-- ARGV[2] delivery_token_digest
-- ARGV[3] operation_id
-- ARGV[4] args_digest (over operation_id and the token digest)
-- ARGV[5] extension_ms
-- ARGV[6] max_lifetime_ms
-- ARGV[7] op_ttl_ms (10 minutes)
-- ARGV[8] key prefix ("hr1")
--
-- Preconditions in order: the operation record's type (wrong_type); an
-- existing operation record (operation_conflict when its kind or
-- args_digest differ; a malformed recorded result is wrong_type; else
-- replay of the recorded result — also after the attempt ended, as the
-- recorded response of an idempotent operation); token record and lease
-- index types (wrong_type); token record absent (not_found); a token phase
-- other than active (stale); queue or head state of the wrong type
-- (wrong_type); the token is not the current unexpired leased head (stale);
-- block marker present (recipient_blocked); malformed deadline fields
-- (wrong_type); the lease already reached the maximum lifetime
-- (maximum_lease_lifetime_reached).
--
-- Writes: lease_expires_ms in the head state and the token record, the
-- lease score, the token record TTL (op_ttl_ms beyond the new deadline),
-- and the extend operation record (kind=extend, state=completed,
-- message_id, lease_expires_ms, max_lease_expires_ms; TTL op_ttl_ms).
--
-- Returns:
--   {"extended", message_id, lease_expires_ms, max_lease_expires_ms, recipient_identity, delivery_cycle, attempt}
--   {"replay", message_id, lease_expires_ms, max_lease_expires_ms}
--   {"operation_conflict"}
--   {"not_found"}
--   {"stale"}
--   {"recipient_blocked"}
--   {"maximum_lease_lifetime_reached"}
--   {"wrong_type"}

if #KEYS ~= 3 or #ARGV ~= 8 then
  return redis.error_reply('ERR extend_v1: expected 3 keys and 8 arguments')
end
for i = 1, 8 do
  if ARGV[i] == '' then
    return redis.error_reply('ERR extend_v1: argument ' .. i .. ' is empty')
  end
end
for _, i in ipairs({5, 6, 7}) do
  if not string.match(ARGV[i], '^[1-9][0-9]*$') or #ARGV[i] > 15 then
    return redis.error_reply('ERR extend_v1: argument ' .. i .. ' must be a positive integer')
  end
end
local prefix = ARGV[8]
if KEYS[1] ~= prefix .. ':t:' .. ARGV[2] or KEYS[2] ~= prefix .. ':op:' .. ARGV[3] or KEYS[3] ~= prefix .. ':leases' then
  return redis.error_reply('ERR extend_v1: keys do not match the token, operation, and prefix')
end
local tokenKey, opKey, leasesKey = KEYS[1], KEYS[2], KEYS[3]

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

-- 1. Operation replay.
if not has_type(opKey, 'hash') then
  return {'wrong_type'}
end
if type_of(opKey) ~= 'none' then
  local op = redis.call('HMGET', opKey, 'kind', 'args_digest', 'message_id', 'lease_expires_ms', 'max_lease_expires_ms')
  if op[1] ~= 'extend' or op[2] ~= ARGV[4] then
    return {'operation_conflict'}
  end
  if not (op[3] and op[3] ~= '' and valid_ms(op[4]) and valid_ms(op[5])) then
    return {'wrong_type'}
  end
  return {'replay', op[3], tonumber(op[4]), tonumber(op[5])}
end
if not (has_type(tokenKey, 'hash') and has_type(leasesKey, 'zset')) then
  return {'wrong_type'}
end

-- 2. Token record.
if type_of(tokenKey) == 'none' then
  return {'not_found'}
end
local tok = redis.call('HMGET', tokenKey, 'state', 'recipient_identity', 'message_id')
if tok[1] ~= 'active' or not tok[2] or not tok[3] then
  return {'stale'}
end
local rid, messageID = tok[2], tok[3]
local queueKey = prefix .. ':r:' .. rid .. ':q'
local stateKey = prefix .. ':r:' .. rid .. ':s'
local markerKey = prefix .. ':q:' .. rid
if not (has_type(queueKey, 'list') and has_type(stateKey, 'hash')) then
  return {'wrong_type'}
end

-- 3. The token must be the current, unexpired lease of the queue head.
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local state = redis.call('HMGET', stateKey, 'status', 'delivery_token', 'head_message_id', 'lease_expires_ms',
  'attempt_started_ms', 'claimed_ms', 'delivery_cycle', 'attempt')
if state[1] ~= 'leased' or state[2] ~= ARGV[1] or state[3] ~= messageID
    or redis.call('LINDEX', queueKey, 0) ~= messageID or (tonumber(state[4]) or 0) <= now_ms then
  return {'stale'}
end
if type_of(markerKey) ~= 'none' then
  return {'recipient_blocked'}
end

-- 4. The maximum lifetime.
local started = state[5] or state[6]
if not (valid_ms(state[4]) and valid_ms(started) and valid_ms(state[7]) and valid_ms(state[8])) then
  return {'wrong_type'}
end
local lease = tonumber(state[4])
local maxLease = tonumber(started) + tonumber(ARGV[6])
if lease >= maxLease then
  return {'maximum_lease_lifetime_reached'}
end
local newLease = math.min(lease + tonumber(ARGV[5]), maxLease)

-- Writes.
redis.call('HSET', stateKey, 'lease_expires_ms', newLease)
redis.call('ZADD', leasesKey, newLease, rid)
redis.call('HSET', tokenKey, 'lease_expires_ms', newLease)
redis.call('PEXPIRE', tokenKey, newLease - now_ms + tonumber(ARGV[7]))
redis.call('HSET', opKey,
  'kind', 'extend',
  'args_digest', ARGV[4],
  'state', 'completed',
  'message_id', messageID,
  'lease_expires_ms', newLease,
  'max_lease_expires_ms', maxLease)
redis.call('PEXPIRE', opKey, tonumber(ARGV[7]))

return {'extended', messageID, newLease, maxLease, rid, tonumber(state[7]), tonumber(state[8])}
