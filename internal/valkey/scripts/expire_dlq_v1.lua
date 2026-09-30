-- expire_dlq_v1: delete one Dead-letter Message whose retention has ended,
-- with its Canonical Message, message metadata, and attempt history, and
-- append the required audit event (without payload) in the same
-- operation. Cooperative maintenance calls it for members of hr1:dlq; the
-- member is only a locator, so the script re-validates the authoritative
-- record and Valkey TIME before mutating.
--
-- KEYS[1] DLQ index     hr1:dlq
-- KEYS[2] audit stream  hr1:audit
--
-- ARGV[1] message_id
-- ARGV[2] retention_ms
-- ARGV[3] event_id
-- ARGV[4] key prefix ("hr1")
--
-- Preconditions in order: the DLQ index or audit stream of the wrong type
-- (wrong_type); hr1:dl absent (stale; the orphan dlq member is removed);
-- hr1:dl not a Hash or without a valid dead_lettered_ms (wrong_type);
-- dead_lettered_ms + retention_ms > TIME (not_due; nothing changes).
--
-- Writes: DEL hr1:dl, hr1:m, hr1:mi, hr1:a; ZREM dlq; XADD audit
-- actor=maintenance, operation=dead_letter_expired, target=<message_id>,
-- reason=<dead_letter_reason>.
--
-- Returns:
--   {"expired", dead_lettered_ms, recipient_identity, dead_letter_reason, expired_ms}
--   {"not_due"}
--   {"stale"}
--   {"wrong_type"}

if #KEYS ~= 2 or #ARGV ~= 4 then
  return redis.error_reply('ERR expire_dlq_v1: expected 2 keys and 4 arguments')
end
for i = 1, 4 do
  if ARGV[i] == '' then
    return redis.error_reply('ERR expire_dlq_v1: argument ' .. i .. ' is empty')
  end
end
if not string.match(ARGV[2], '^[1-9][0-9]*$') or #ARGV[2] > 15 then
  return redis.error_reply('ERR expire_dlq_v1: retention_ms must be a positive integer')
end
local messageID, prefix = ARGV[1], ARGV[4]
if KEYS[1] ~= prefix .. ':dlq' or KEYS[2] ~= prefix .. ':audit' then
  return redis.error_reply('ERR expire_dlq_v1: keys do not match the prefix')
end
local dlqKey, auditKey = KEYS[1], KEYS[2]
local dlKey = prefix .. ':dl:' .. messageID

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

-- 1. Global structures.
if not (has_type(dlqKey, 'zset') and has_type(auditKey, 'stream')) then
  return {'wrong_type'}
end

-- 2. The authoritative record.
local dt = type_of(dlKey)
if dt == 'none' then
  redis.call('ZREM', dlqKey, messageID)
  return {'stale'}
end
if dt ~= 'hash' then
  return {'wrong_type'}
end
local dl = redis.call('HMGET', dlKey, 'dead_lettered_ms', 'recipient_identity', 'dead_letter_reason')
if not valid_ms(dl[1]) then
  return {'wrong_type'}
end

-- 3. Retention.
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
if tonumber(dl[1]) + tonumber(ARGV[2]) > now_ms then
  return {'not_due'}
end

-- Writes.
redis.call('DEL', dlKey, prefix .. ':m:' .. messageID, prefix .. ':mi:' .. messageID, prefix .. ':a:' .. messageID)
redis.call('ZREM', dlqKey, messageID)
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[3],
  'timestamp_ms', now_ms,
  'actor', 'maintenance',
  'operation', 'dead_letter_expired',
  'target', messageID,
  'outcome', 'success',
  'reason', dl[3] or '')

return {'expired', tonumber(dl[1]), dl[2] or '', dl[3] or '', now_ms}
