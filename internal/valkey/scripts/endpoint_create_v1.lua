-- endpoint_create_v1: create one Webhook Endpoint with its mandatory
-- administrative audit append in one atomic operation. Validates key types
-- and preconditions before any write; Valkey TIME is authoritative for
-- created_ms, updated_ms, and the audit timestamp.
--
-- KEYS[1] endpoint hash   hr1:wh:<webhook_type>:<webhook_identifier>
-- KEYS[2] bot set         hr1:bot:<bot_platform>:<bot_id>:webhooks
-- KEYS[3] global listing  hr1:webhooks
-- KEYS[4] audit stream    hr1:audit
--
-- ARGV[1] webhook_type
-- ARGV[2] webhook_identifier
-- ARGV[3] bot_platform (derived by the caller from the type mapping)
-- ARGV[4] bot_id
-- ARGV[5] enabled ("0"|"1")
-- ARGV[6] credential_kind
-- ARGV[7] credential_value (plaintext, secret-bearing infrastructure)
-- ARGV[8] generation_id (UUIDv7, caller-generated)
-- ARGV[9] event_id (UUIDv7, caller-generated audit identity)
-- ARGV[10] operation (bounded audit operation name)
-- ARGV[11] request_id
--
-- Returns:
--   {"created", created_ms, updated_ms}
--   {"conflict"}                                   endpoint already exists
--   {"bot_endpoint_limit"}                         bot set at the 100 cap
--   {"wrong_type"}                                 a key has an unexpected type

local endpointKey = KEYS[1]
local botKey = KEYS[2]
local listingKey = KEYS[3]
local auditKey = KEYS[4]

-- Pre-write validation: expected key types before any write.
local t = redis.call('TYPE', endpointKey)['ok']
if t ~= 'none' and t ~= 'hash' then
  return {'wrong_type'}
end
t = redis.call('TYPE', botKey)['ok']
if t ~= 'none' and t ~= 'set' then
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

if redis.call('EXISTS', endpointKey) == 1 then
  return {'conflict'}
end
if redis.call('SCARD', botKey) >= 100 then
  return {'bot_endpoint_limit'}
end

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

local member = ARGV[1] .. ':' .. ARGV[2]

redis.call('HSET', endpointKey,
  'bot_id', ARGV[4],
  'enabled', ARGV[5],
  'credential_kind', ARGV[6],
  'credential_value', ARGV[7],
  'generation_id', ARGV[8],
  'created_ms', now_ms,
  'updated_ms', now_ms,
  'config_version', 1)
redis.call('SADD', botKey, member)
redis.call('ZADD', listingKey, now_ms, member)
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[9],
  'timestamp_ms', now_ms,
  'actor', 'admin_bearer',
  'operation', ARGV[10],
  'target', member,
  'request_id', ARGV[11],
  'outcome', 'success')

return {'created', now_ms, now_ms}
