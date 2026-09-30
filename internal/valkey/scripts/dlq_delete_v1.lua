-- dlq_delete_v1: permanently delete one Dead-letter Message the operator
-- reviewed, with its Canonical Message, message metadata, and attempt
-- history, and append the mandatory audit event (without payload) in the
-- same operation. The expected (delivery_cycle, dead_lettered_ms) pair is
-- the entity tag of the reviewed entry: a message replayed and
-- dead-lettered again carries a new pair, so a stale decision never
-- deletes it. Deduplication records are not touched (as for retention
-- expiry). The block marker key is resolved from the record's
-- recipient_identity (standalone Valkey is a precondition).
--
-- KEYS[1] DLQ index     hr1:dlq
-- KEYS[2] audit stream  hr1:audit
--
-- ARGV[1] message_id
-- ARGV[2] expected delivery_cycle   (empty together with ARGV[3]: no If-Match)
-- ARGV[3] expected dead_lettered_ms
-- ARGV[4] actor: admin_bearer | admin_session
-- ARGV[5] event_id
-- ARGV[6] request_id
-- ARGV[7] key prefix ("hr1")
--
-- Preconditions in order: the DLQ index or audit stream of the wrong type
-- (wrong_type); hr1:dl absent (absent; a stale index member is removed, no
-- audit); hr1:dl not a Hash or without a valid delivery_cycle,
-- dead_lettered_ms, and recipient_identity (wrong_type); no expected pair
-- (precondition_required); a different pair (precondition_failed with the
-- current pair); a block marker for the Recipient (recipient_blocked).
--
-- Writes: DEL hr1:dl, hr1:m, hr1:mi, hr1:a; ZREM dlq; XADD audit
-- actor=<actor>, operation=dead_letter_deleted, target=<message_id>,
-- reason=<dead_letter_reason>.
--
-- Returns:
--   {"deleted", deleted_ms, recipient_identity, dead_letter_reason}
--   {"absent"}
--   {"precondition_required"}
--   {"precondition_failed", delivery_cycle, dead_lettered_ms}
--   {"recipient_blocked"}
--   {"wrong_type"}

if #KEYS ~= 2 or #ARGV ~= 7 then
  return redis.error_reply('ERR dlq_delete_v1: expected 2 keys and 7 arguments')
end
for _, i in ipairs({1, 4, 5, 6, 7}) do
  if ARGV[i] == '' then
    return redis.error_reply('ERR dlq_delete_v1: argument ' .. i .. ' is empty')
  end
end
local function valid_int(value)
  return value and string.match(value, '^[1-9]%d*$') and #value <= 15
end
local expected = ARGV[2] ~= '' or ARGV[3] ~= ''
if expected and not (valid_int(ARGV[2]) and valid_int(ARGV[3])) then
  return redis.error_reply('ERR dlq_delete_v1: the expected pair must be two positive integers or both empty')
end
if ARGV[4] ~= 'admin_bearer' and ARGV[4] ~= 'admin_session' then
  return redis.error_reply('ERR dlq_delete_v1: actor must be admin_bearer or admin_session')
end
local messageID, prefix = ARGV[1], ARGV[7]
if KEYS[1] ~= prefix .. ':dlq' or KEYS[2] ~= prefix .. ':audit' then
  return redis.error_reply('ERR dlq_delete_v1: keys do not match the prefix')
end
local dlqKey, auditKey = KEYS[1], KEYS[2]
local dlKey = prefix .. ':dl:' .. messageID

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, want)
  local t = type_of(key)
  return t == 'none' or t == want
end

-- 1. Global structures.
if not (has_type(dlqKey, 'zset') and has_type(auditKey, 'stream')) then
  return {'wrong_type'}
end

-- 2. The authoritative record.
local dt = type_of(dlKey)
if dt == 'none' then
  redis.call('ZREM', dlqKey, messageID)
  return {'absent'}
end
if dt ~= 'hash' then
  return {'wrong_type'}
end
local dl = redis.call('HMGET', dlKey, 'delivery_cycle', 'dead_lettered_ms', 'recipient_identity', 'dead_letter_reason')
if not (valid_int(dl[1]) and valid_int(dl[2]) and dl[3] and dl[3] ~= '') then
  return {'wrong_type'}
end

-- 3. The reviewed entry.
if not expected then
  return {'precondition_required'}
end
if dl[1] ~= ARGV[2] or dl[2] ~= ARGV[3] then
  return {'precondition_failed', tonumber(dl[1]), tonumber(dl[2])}
end

-- 4. A blocked Recipient is under incident recovery: keep its evidence.
if type_of(prefix .. ':q:' .. dl[3]) ~= 'none' then
  return {'recipient_blocked'}
end

-- Writes.
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('DEL', dlKey, prefix .. ':m:' .. messageID, prefix .. ':mi:' .. messageID, prefix .. ':a:' .. messageID)
redis.call('ZREM', dlqKey, messageID)
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[5],
  'timestamp_ms', now_ms,
  'actor', ARGV[4],
  'operation', 'dead_letter_deleted',
  'target', messageID,
  'request_id', ARGV[6],
  'outcome', 'success',
  'reason', dl[4] or '')

return {'deleted', now_ms, dl[3], dl[4] or ''}
