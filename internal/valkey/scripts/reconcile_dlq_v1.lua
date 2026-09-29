-- reconcile_dlq_v1: verify a batch of dead-letter entries in both
-- directions and atomically repair only the derived hr1:dlq index. The
-- dead-letter Hash hr1:dl:<message_id> is authoritative and never
-- rewritten. A dead-letter entry whose Canonical Message is missing is not
-- an isolatable Recipient ambiguity: it is reported (with its message id)
-- for a readiness hold, and nothing is changed for it — in particular no
-- Recipient block marker is created.
--
-- KEYS[1] DLQ index  hr1:dlq
--
-- ARGV[1]    key prefix ("hr1")
-- ARGV[2..]  message ids (members of hr1:dlq and/or ids of hr1:dl:* keys)
--
-- Per id: no hr1:dl Hash -> the stale index member is removed (orphan);
-- hr1:dl of another type, or missing/malformed required fields
-- (bot_platform, bot_id, recipient_scope, chat_id for chat scope, user_id
-- for user scope, recipient_identity, dead_lettered_ms, dead_letter_reason
-- nack_exhausted|expiry_exhausted, delivery_cycle) -> invalid (nothing
-- changes); blob hr1:m:<id> not a string -> missing (nothing changes);
-- index member missing or scored differently from dead_lettered_ms ->
-- restored.
--
-- Returns (each list holds message ids):
--   {"reconciled", {orphans removed}, {restored}, {invalid}, {missing}}
--   {"wrong_type"}  the DLQ index itself has the wrong type (no mutation)

if #KEYS ~= 1 or #ARGV < 2 or ARGV[1] == '' then
  return redis.error_reply('ERR reconcile_dlq_v1: expected 1 key, a prefix, and at least one message id')
end
local prefix = ARGV[1]
if KEYS[1] ~= prefix .. ':dlq' then
  return redis.error_reply('ERR reconcile_dlq_v1: key does not match the prefix')
end
local dlqKey = KEYS[1]

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local dt = type_of(dlqKey)
if dt ~= 'none' and dt ~= 'zset' then
  return {'wrong_type'}
end

local removed, restored, invalid, missing = {}, {}, {}, {}
local function nonempty(v)
  return v and v ~= ''
end
local function valid_int(v)
  return v and string.match(v, '^[1-9]%d*$') and #v <= 15
end
local seen = {}
for i = 2, #ARGV do
  local id = ARGV[i]
  if id ~= '' and not seen[id] then
    seen[id] = true
    local dlKey = prefix .. ':dl:' .. id
    local t = type_of(dlKey)
    if t == 'none' then
      if redis.call('ZREM', dlqKey, id) == 1 then
        removed[#removed + 1] = id
      end
    elseif t ~= 'hash' then
      invalid[#invalid + 1] = id
    else
      local f = redis.call('HMGET', dlKey, 'bot_platform', 'bot_id', 'recipient_scope', 'chat_id', 'user_id',
        'recipient_identity', 'dead_lettered_ms', 'dead_letter_reason', 'delivery_cycle')
      local scope = f[3]
      local scoped = (scope == 'chat' and nonempty(f[4])) or (scope == 'user' and nonempty(f[5]))
        or scope == 'bot' or scope == 'relay'
      if not (nonempty(f[1]) and nonempty(f[2]) and scoped and nonempty(f[6]) and valid_int(f[7])
          and (f[8] == 'nack_exhausted' or f[8] == 'expiry_exhausted') and valid_int(f[9])) then
        invalid[#invalid + 1] = id
      elseif type_of(prefix .. ':m:' .. id) ~= 'string' then
        missing[#missing + 1] = id
      elseif tonumber(redis.call('ZSCORE', dlqKey, id)) ~= tonumber(f[7]) then
        redis.call('ZADD', dlqKey, f[7], id)
        restored[#restored + 1] = id
      end
    end
  end
end
return {'reconciled', removed, restored, invalid, missing}
