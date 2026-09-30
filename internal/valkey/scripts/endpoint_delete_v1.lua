-- endpoint_delete_v1: permanently delete one disabled Webhook Endpoint under
-- a strong entity-version precondition, with its mandatory administrative
-- audit append in one atomic operation. Removes the endpoint Hash (and its
-- plaintext credential), the Bot Identity Set membership (deleting an empty
-- Set), and the global listing member; no tombstone is written. An absent
-- endpoint is a no-op without audit. The Bot Identity Set key is resolved
-- from the Hash's bot_id (standalone Valkey) and verified to be a Set.
--
-- KEYS[1] endpoint hash   hr1:wh:<webhook_type>:<webhook_identifier>
-- KEYS[2] global listing  hr1:webhooks
-- KEYS[3] audit stream    hr1:audit
--
-- ARGV[1] webhook_type
-- ARGV[2] webhook_identifier
-- ARGV[3] bot_platform (derived by the caller from the type mapping)
-- ARGV[4] expected generation_id ("" when no If-Match was sent)
-- ARGV[5] expected config_version ("" exactly when ARGV[4] is "")
-- ARGV[6] event_id (UUIDv7, caller-generated audit identity)
-- ARGV[7] request_id
--
-- Returns:
--   {"deleted", bot_id, credential_kind, generation_id, config_version, deleted_ms}
--   {"absent"}
--   {"precondition_required"}                        no expected version
--   {"precondition_failed", generation_id, config_version}
--   {"must_be_disabled"}
--   {"wrong_type"}                                   unexpected key type or
--                                                    malformed endpoint Hash

if #KEYS ~= 3 or #ARGV ~= 7 then
  return redis.error_reply('ERR endpoint_delete_v1: expected 3 keys and 7 arguments')
end
for _, i in ipairs({1, 2, 3, 6, 7}) do
  if ARGV[i] == '' then
    return redis.error_reply('ERR endpoint_delete_v1: argument ' .. i .. ' is empty')
  end
end
if (ARGV[4] == '') ~= (ARGV[5] == '') or (ARGV[5] ~= '' and not string.match(ARGV[5], '^[1-9][0-9]*$')) then
  return redis.error_reply('ERR endpoint_delete_v1: the expected version is incomplete or malformed')
end
if KEYS[1] ~= 'hr1:wh:' .. ARGV[1] .. ':' .. ARGV[2] then
  return redis.error_reply('ERR endpoint_delete_v1: key does not match the endpoint identity')
end

local endpointKey, listingKey, auditKey = KEYS[1], KEYS[2], KEYS[3]

local t = redis.call('TYPE', endpointKey)['ok']
if t ~= 'none' and t ~= 'hash' then
  return {'wrong_type'}
end
t = redis.call('TYPE', listingKey)['ok']
if t ~= 'none' and t ~= 'zset' then
  return {'wrong_type'}
end
t = redis.call('TYPE', auditKey)['ok']
if t ~= 'none' and t ~= 'stream' then
  return {'wrong_type'}
end
if redis.call('EXISTS', endpointKey) == 0 then
  return {'absent'}
end

local f = redis.call('HMGET', endpointKey, 'bot_id', 'enabled', 'credential_kind', 'generation_id', 'config_version')
local botID, enabled, kind, generation, version = f[1], f[2], f[3], f[4], f[5]
if not botID or not kind or not generation or (enabled ~= '0' and enabled ~= '1')
    or not version or not string.match(version, '^[1-9][0-9]*$') then
  return {'wrong_type'}
end
local botKey = 'hr1:bot:' .. ARGV[3] .. ':' .. botID .. ':webhooks'
t = redis.call('TYPE', botKey)['ok']
if t ~= 'none' and t ~= 'set' then
  return {'wrong_type'}
end

if ARGV[4] == '' then
  return {'precondition_required'}
end
if ARGV[4] ~= generation or ARGV[5] ~= version then
  return {'precondition_failed', generation, version}
end
if enabled == '1' then
  return {'must_be_disabled'}
end

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local member = ARGV[1] .. ':' .. ARGV[2]

redis.call('DEL', endpointKey)
redis.call('SREM', botKey, member)
if redis.call('SCARD', botKey) == 0 then
  redis.call('DEL', botKey)
end
redis.call('ZREM', listingKey, member)
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[6],
  'timestamp_ms', now_ms,
  'actor', 'admin_bearer',
  'operation', 'webhook_endpoint_deleted',
  'target', member,
  'request_id', ARGV[7],
  'outcome', 'success')

return {'deleted', botID, kind, generation, version, tostring(now_ms)}
