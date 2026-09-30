-- endpoint_set_enabled_v1: change the enabled flag of one Webhook Endpoint
-- under a strong entity-version precondition, with its mandatory
-- administrative audit append in one atomic operation. Validates arguments,
-- key types, and preconditions before any write; Valkey TIME is
-- authoritative for updated_ms and the audit timestamp. Setting the flag to
-- its current value writes nothing and appends no audit.
--
-- KEYS[1] endpoint hash   hr1:wh:<webhook_type>:<webhook_identifier>
-- KEYS[2] audit stream    hr1:audit
--
-- ARGV[1] webhook_type
-- ARGV[2] webhook_identifier
-- ARGV[3] enabled ("0"|"1")
-- ARGV[4] expected generation_id ("" when no If-Match was sent)
-- ARGV[5] expected config_version ("" exactly when ARGV[4] is "")
-- ARGV[6] event_id (UUIDv7, caller-generated audit identity)
-- ARGV[7] request_id
--
-- Returns (the safe fields never include the credential value):
--   {"updated", bot_id, enabled, credential_kind, generation_id, created_ms, updated_ms, config_version}
--   {"unchanged", <same seven fields>}
--   {"not_found"}
--   {"precondition_required"}                        no expected version
--   {"precondition_failed", generation_id, config_version}
--   {"wrong_type"}                                   unexpected key type or
--                                                    malformed endpoint Hash

if #KEYS ~= 2 or #ARGV ~= 7 then
  return redis.error_reply('ERR endpoint_set_enabled_v1: expected 2 keys and 7 arguments')
end
for _, i in ipairs({1, 2, 3, 6, 7}) do
  if ARGV[i] == '' then
    return redis.error_reply('ERR endpoint_set_enabled_v1: argument ' .. i .. ' is empty')
  end
end
if ARGV[3] ~= '0' and ARGV[3] ~= '1' then
  return redis.error_reply('ERR endpoint_set_enabled_v1: enabled must be 0 or 1')
end
if (ARGV[4] == '') ~= (ARGV[5] == '') or (ARGV[5] ~= '' and not string.match(ARGV[5], '^[1-9][0-9]*$')) then
  return redis.error_reply('ERR endpoint_set_enabled_v1: the expected version is incomplete or malformed')
end
if KEYS[1] ~= 'hr1:wh:' .. ARGV[1] .. ':' .. ARGV[2] then
  return redis.error_reply('ERR endpoint_set_enabled_v1: key does not match the endpoint identity')
end

local endpointKey, auditKey = KEYS[1], KEYS[2]

local t = redis.call('TYPE', endpointKey)['ok']
if t ~= 'none' and t ~= 'hash' then
  return {'wrong_type'}
end
t = redis.call('TYPE', auditKey)['ok']
if t ~= 'none' and t ~= 'stream' then
  return {'wrong_type'}
end
if redis.call('EXISTS', endpointKey) == 0 then
  return {'not_found'}
end

local f = redis.call('HMGET', endpointKey, 'bot_id', 'enabled', 'credential_kind', 'generation_id', 'created_ms', 'updated_ms', 'config_version')
local botID, enabled, kind, generation, created, updated, version = f[1], f[2], f[3], f[4], f[5], f[6], f[7]
if not botID or not kind or not generation or not created or not updated
    or (enabled ~= '0' and enabled ~= '1') or not version or not string.match(version, '^[1-9][0-9]*$') then
  return {'wrong_type'}
end

if ARGV[4] == '' then
  return {'precondition_required'}
end
if ARGV[4] ~= generation or ARGV[5] ~= version then
  return {'precondition_failed', generation, version}
end
if enabled == ARGV[3] then
  return {'unchanged', botID, enabled, kind, generation, created, updated, version}
end

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local newVersion = tonumber(version) + 1
local operation = 'webhook_endpoint_disabled'
if ARGV[3] == '1' then
  operation = 'webhook_endpoint_enabled'
end

redis.call('HSET', endpointKey, 'enabled', ARGV[3], 'config_version', newVersion, 'updated_ms', now_ms)
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[6],
  'timestamp_ms', now_ms,
  'actor', 'admin_bearer',
  'operation', operation,
  'target', ARGV[1] .. ':' .. ARGV[2],
  'request_id', ARGV[7],
  'outcome', 'success')

return {'updated', botID, ARGV[3], kind, generation, created, tostring(now_ms), tostring(newVersion)}
