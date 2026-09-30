-- reconcile_endpoint_v1: reconcile the two derived Webhook Endpoint indexes
-- from authoritative endpoint Hashes, or remove an orphan/mismatched member
-- found while scanning an index. Endpoint fields and credentials are never
-- changed. Per-endpoint and Bot Identity keys are resolved inside the script
-- (standalone Valkey).
--
-- KEYS[1] global listing  hr1:webhooks
--
-- ARGV[1] mode: bot_member | listing_member | endpoint
-- ARGV[2] member: <webhook_type>:<webhook_identifier>
-- ARGV[3] scanned bot_platform (bot_member only; empty otherwise)
-- ARGV[4] scanned bot_id (bot_member only; empty otherwise)
-- ARGV[5] key prefix ("hr1")
--
-- Returns:
--   {"consistent"}
--   {"repaired", bot_membership_repaired, listing_repair}
--     listing_repair: 0 none, 1 restored, 2 score repaired
--   {"orphan_removed", reason}
--   {"invalid", reason}
--   {"wrong_type", reason}
--   {"absent"}

if #KEYS ~= 1 or #ARGV ~= 5 then
  return redis.error_reply('ERR reconcile_endpoint_v1: expected 1 key and 5 arguments')
end
local mode, member, scannedPlatform, scannedBotID, prefix = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]
if prefix == '' or member == '' or (mode ~= 'bot_member' and mode ~= 'listing_member' and mode ~= 'endpoint') then
  return redis.error_reply('ERR reconcile_endpoint_v1: invalid arguments')
end
if KEYS[1] ~= prefix .. ':webhooks' then
  return redis.error_reply('ERR reconcile_endpoint_v1: key does not match the prefix')
end
if mode == 'bot_member' then
  if scannedPlatform == '' or scannedBotID == '' then
    return redis.error_reply('ERR reconcile_endpoint_v1: bot_member requires a Bot Identity')
  end
elseif scannedPlatform ~= '' or scannedBotID ~= '' then
  return redis.error_reply('ERR reconcile_endpoint_v1: scanned Bot Identity is only valid in bot_member mode')
end

local listingKey = KEYS[1]
local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function valid_identifier(value)
  return value and #value >= 1 and #value <= 128 and string.match(value, '^[A-Za-z0-9_-]+$')
end
local function valid_bot_id(value)
  return value and #value >= 1 and #value <= 20 and string.match(value, '^[1-9][0-9]*$')
end
local function valid_credential(value)
  return value and #value >= 1 and #value <= 256 and string.match(value, '^[A-Za-z0-9_-]+$')
end
local function valid_positive(value, maxlen)
  return value and #value <= maxlen and string.match(value, '^[1-9][0-9]*$')
end
local function valid_uuid_v7(value)
  if not value or #value ~= 36 or string.sub(value, 9, 9) ~= '-' or string.sub(value, 14, 14) ~= '-'
      or string.sub(value, 19, 19) ~= '-' or string.sub(value, 24, 24) ~= '-' or string.sub(value, 15, 15) ~= '7' then
    return false
  end
  local compact = string.gsub(value, '-', '')
  return #compact == 32 and string.match(compact, '^[0-9a-fA-F]+$') ~= nil
end
local function parse_member(value)
  local webhookType, identifier = string.match(value, '^([^:]+):([^:]+)$')
  if webhookType ~= 'telegram' or not valid_identifier(identifier) then
    return nil, nil
  end
  return webhookType, identifier
end
local function endpoint_record(endpointKey)
  local t = type_of(endpointKey)
  if t == 'none' then
    return nil, 'absent'
  end
  if t ~= 'hash' then
    return nil, 'endpoint_type'
  end
  local f = redis.call('HMGET', endpointKey, 'bot_id', 'enabled', 'credential_kind', 'credential_value',
    'generation_id', 'created_ms', 'updated_ms', 'config_version')
  if not valid_bot_id(f[1]) or (f[2] ~= '0' and f[2] ~= '1') or f[3] ~= 'secret_token'
      or not valid_credential(f[4]) or not valid_uuid_v7(f[5]) or not valid_positive(f[6], 15)
      or not valid_positive(f[7], 15) or not valid_positive(f[8], 19)
      or tonumber(f[7]) < tonumber(f[6]) then
    return nil, 'endpoint_record_invalid'
  end
  return {bot_id = f[1], created_ms = f[6]}, nil
end

if mode == 'bot_member' then
  if scannedPlatform ~= 'telegram' or not valid_bot_id(scannedBotID) then
    return redis.error_reply('ERR reconcile_endpoint_v1: invalid scanned Bot Identity')
  end
  local scannedBotKey = prefix .. ':bot:' .. scannedPlatform .. ':' .. scannedBotID .. ':webhooks'
  local bt = type_of(scannedBotKey)
  if bt == 'none' then
    return {'consistent'}
  end
  if bt ~= 'set' then
    return {'wrong_type', 'bot_index_type'}
  end
  local webhookType, identifier = parse_member(member)
  if not webhookType then
    redis.call('SREM', scannedBotKey, member)
    return {'orphan_removed', 'bot_member_malformed'}
  end
  local rec, issue = endpoint_record(prefix .. ':wh:' .. webhookType .. ':' .. identifier)
  if issue == 'absent' then
    redis.call('SREM', scannedBotKey, member)
    return {'orphan_removed', 'bot_member_orphan'}
  end
  if issue == 'endpoint_type' then
    return {'wrong_type', issue}
  end
  if issue then
    return {'invalid', issue}
  end
  if rec.bot_id ~= scannedBotID then
    redis.call('SREM', scannedBotKey, member)
    if redis.call('SCARD', scannedBotKey) == 0 then
      redis.call('DEL', scannedBotKey)
    end
    return {'orphan_removed', 'bot_member_mismatch'}
  end
  return {'consistent'}
end

local lt = type_of(listingKey)
if lt ~= 'none' and lt ~= 'zset' then
  return {'wrong_type', 'listing_type'}
end
local webhookType, identifier = parse_member(member)
if mode == 'listing_member' then
  if not webhookType then
    redis.call('ZREM', listingKey, member)
    return {'orphan_removed', 'listing_member_malformed'}
  end
  local rec, issue = endpoint_record(prefix .. ':wh:' .. webhookType .. ':' .. identifier)
  if issue == 'absent' then
    redis.call('ZREM', listingKey, member)
    return {'orphan_removed', 'listing_member_orphan'}
  end
  if issue == 'endpoint_type' then
    return {'wrong_type', issue}
  end
  if issue then
    return {'invalid', issue}
  end
  local score = redis.call('ZSCORE', listingKey, member)
  if not score then
    redis.call('ZADD', listingKey, rec.created_ms, member)
    return {'repaired', 0, 1}
  end
  if tonumber(score) ~= tonumber(rec.created_ms) then
    redis.call('ZADD', listingKey, rec.created_ms, member)
    return {'repaired', 0, 2}
  end
  return {'consistent'}
end

if not webhookType then
  return {'invalid', 'endpoint_key_invalid'}
end
local endpointKey = prefix .. ':wh:' .. webhookType .. ':' .. identifier
local rec, issue = endpoint_record(endpointKey)
if issue == 'absent' then
  return {'absent'}
end
if issue == 'endpoint_type' then
  return {'wrong_type', issue}
end
if issue then
  return {'invalid', issue}
end
local botKey = prefix .. ':bot:telegram:' .. rec.bot_id .. ':webhooks'
local bt = type_of(botKey)
if bt ~= 'none' and bt ~= 'set' then
  return {'wrong_type', 'bot_index_type'}
end
local inBot = redis.call('SISMEMBER', botKey, member)
local botCount = redis.call('SCARD', botKey)
if botCount > 100 or (inBot == 0 and botCount >= 100) then
  return {'invalid', 'bot_endpoint_limit'}
end

local botRepair = 0
if inBot == 0 then
  redis.call('SADD', botKey, member)
  botRepair = 1
end
local listingRepair = 0
local score = redis.call('ZSCORE', listingKey, member)
if not score then
  redis.call('ZADD', listingKey, rec.created_ms, member)
  listingRepair = 1
elseif tonumber(score) ~= tonumber(rec.created_ms) then
  redis.call('ZADD', listingKey, rec.created_ms, member)
  listingRepair = 2
end
if botRepair > 0 or listingRepair > 0 then
  return {'repaired', botRepair, listingRepair}
end
return {'consistent'}
