-- dlq_payload_v1: disclose one Dead-letter Message's Canonical Message to
-- an authenticated administrator after appending the mandatory access
-- audit event in the same operation. The XADD precedes the return, so the
-- blob is never returned unless the append happened. The event carries
-- safe metadata only, never the payload.
--
-- KEYS[1] audit stream  hr1:audit
--
-- ARGV[1] message_id
-- ARGV[2] actor: admin_bearer | admin_session
-- ARGV[3] event_id
-- ARGV[4] request_id
-- ARGV[5] key prefix ("hr1")
--
-- Preconditions in order: the audit stream of the wrong type
-- (wrong_type); hr1:dl absent (not_found; no audit); hr1:dl not a Hash or
-- without a valid delivery_cycle, dead_lettered_ms, and recipient_identity
-- (wrong_type); blob absent (message_missing; no audit, no disclosure) or
-- not a String (wrong_type).
--
-- Writes: XADD audit actor=<actor>, operation=dead_letter_payload_viewed,
-- target=<message_id>.
--
-- Returns:
--   {"disclosed", blob, delivery_cycle, dead_lettered_ms, recipient_identity}
--   {"not_found"}
--   {"message_missing"}
--   {"wrong_type"}

if #KEYS ~= 1 or #ARGV ~= 5 then
  return redis.error_reply('ERR dlq_payload_v1: expected 1 key and 5 arguments')
end
for i = 1, 5 do
  if ARGV[i] == '' then
    return redis.error_reply('ERR dlq_payload_v1: argument ' .. i .. ' is empty')
  end
end
if ARGV[2] ~= 'admin_bearer' and ARGV[2] ~= 'admin_session' then
  return redis.error_reply('ERR dlq_payload_v1: actor must be admin_bearer or admin_session')
end
local messageID, prefix = ARGV[1], ARGV[5]
if KEYS[1] ~= prefix .. ':audit' then
  return redis.error_reply('ERR dlq_payload_v1: keys do not match the prefix')
end
local auditKey = KEYS[1]
local dlKey = prefix .. ':dl:' .. messageID
local blobKey = prefix .. ':m:' .. messageID

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function valid_int(value)
  return value and string.match(value, '^[1-9]%d*$') and #value <= 15
end

-- 1. Global structures.
local at = type_of(auditKey)
if at ~= 'none' and at ~= 'stream' then
  return {'wrong_type'}
end

-- 2. The authoritative dead-letter record.
local dt = type_of(dlKey)
if dt == 'none' then
  return {'not_found'}
end
if dt ~= 'hash' then
  return {'wrong_type'}
end
local dl = redis.call('HMGET', dlKey, 'delivery_cycle', 'dead_lettered_ms', 'recipient_identity')
if not (valid_int(dl[1]) and valid_int(dl[2]) and dl[3] and dl[3] ~= '') then
  return {'wrong_type'}
end

-- 3. The Canonical Message.
local bt = type_of(blobKey)
if bt == 'none' then
  return {'message_missing'}
end
if bt ~= 'string' then
  return {'wrong_type'}
end
local blob = redis.call('GET', blobKey)

-- Audit first, then disclose.
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[3],
  'timestamp_ms', now_ms,
  'actor', ARGV[2],
  'operation', 'dead_letter_payload_viewed',
  'target', messageID,
  'request_id', ARGV[4],
  'outcome', 'success')

return {'disclosed', blob, tonumber(dl[1]), tonumber(dl[2]), dl[3]}
