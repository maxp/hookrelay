-- admin_auth_v1: initialize or rotate the Admin Secret generation record
-- hr1:admin_auth and append the mandatory audit event in the same
-- operation. The record never holds the secret: generation_tag is an
-- HMAC-SHA-256 keyed by the Admin Secret over the stored random salt,
-- computed by the caller. A rotation revokes every indexed browser
-- session; sessions of an older generation that escaped the index are
-- invalid anyway because every session request compares generation_id.
--
-- KEYS[1] generation record  hr1:admin_auth
-- KEYS[2] session index      hr1:admin_sessions
-- KEYS[3] audit stream       hr1:audit
--
-- ARGV[1] mode: initialize | rotate
-- ARGV[2] expected generation_id (rotate; empty for initialize)
-- ARGV[3] new generation_id
-- ARGV[4] generation_salt (initialize: the new salt; rotate: the stored salt)
-- ARGV[5] generation_tag (64 lowercase hex characters)
-- ARGV[6] event_id
-- ARGV[7] key prefix ("hr1")
--
-- Preconditions in order: any key of the wrong type (wrong_type).
-- initialize: record present (exists; no write).
-- rotate: record absent (absent); a record without a salt, tag, or
-- generation_id (wrong_type); a stored generation_id or salt different
-- from the arguments (changed); a stored tag equal to the new one
-- (current; no write); more than 100 indexed sessions
-- (too_many_sessions; no write).
--
-- Writes: initialize HSETs every field and appends actor=startup,
-- operation=admin_auth_initialized, target=admin_auth. rotate DELs every
-- indexed session Hash and the index, HSETs generation_tag,
-- generation_id, updated_ms, and appends
-- operation=admin_secret_generation_changed with
-- reason=sessions_revoked_<0|1_10|11_100>.
--
-- Returns:
--   {"initialized"}
--   {"exists"}
--   {"current"}
--   {"rotated", revoked_count}
--   {"changed"}
--   {"absent"}
--   {"too_many_sessions"}
--   {"wrong_type"}

if #KEYS ~= 3 or #ARGV ~= 7 then
  return redis.error_reply('ERR admin_auth_v1: expected 3 keys and 7 arguments')
end
local mode, prefix = ARGV[1], ARGV[7]
if mode ~= 'initialize' and mode ~= 'rotate' then
  return redis.error_reply('ERR admin_auth_v1: mode must be initialize or rotate')
end
for _, i in ipairs({3, 4, 5, 6, 7}) do
  if ARGV[i] == '' then
    return redis.error_reply('ERR admin_auth_v1: argument ' .. i .. ' is empty')
  end
end
if (mode == 'rotate') ~= (ARGV[2] ~= '') then
  return redis.error_reply('ERR admin_auth_v1: expected generation_id is required exactly for rotate')
end
if not string.match(ARGV[5], '^[0-9a-f]+$') or #ARGV[5] ~= 64 then
  return redis.error_reply('ERR admin_auth_v1: generation_tag must be 64 lowercase hex characters')
end
if KEYS[1] ~= prefix .. ':admin_auth' or KEYS[2] ~= prefix .. ':admin_sessions' or KEYS[3] ~= prefix .. ':audit' then
  return redis.error_reply('ERR admin_auth_v1: keys do not match the prefix')
end
local authKey, indexKey, auditKey = KEYS[1], KEYS[2], KEYS[3]

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, want)
  local t = type_of(key)
  return t == 'none' or t == want
end
if not (has_type(authKey, 'hash') and has_type(indexKey, 'zset') and has_type(auditKey, 'stream')) then
  return {'wrong_type'}
end

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local present = type_of(authKey) ~= 'none'

if mode == 'initialize' then
  if present then
    return {'exists'}
  end
  redis.call('HSET', authKey,
    'generation_salt', ARGV[4],
    'generation_tag', ARGV[5],
    'generation_id', ARGV[3],
    'updated_ms', now_ms)
  redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
    'event_id', ARGV[6],
    'timestamp_ms', now_ms,
    'actor', 'startup',
    'operation', 'admin_auth_initialized',
    'target', 'admin_auth',
    'outcome', 'success')
  return {'initialized'}
end

-- rotate
if not present then
  return {'absent'}
end
local rec = redis.call('HMGET', authKey, 'generation_salt', 'generation_tag', 'generation_id')
if not rec[1] or rec[1] == '' or not rec[2] or rec[2] == '' or not rec[3] or rec[3] == '' then
  return {'wrong_type'}
end
if rec[3] ~= ARGV[2] or rec[1] ~= ARGV[4] then
  return {'changed'}
end
if rec[2] == ARGV[5] then
  return {'current'}
end
local count = redis.call('ZCARD', indexKey)
if count > 100 then
  return {'too_many_sessions'}
end
local members = redis.call('ZRANGE', indexKey, 0, -1)
for _, digest in ipairs(members) do
  redis.call('DEL', prefix .. ':admin_session:' .. digest)
end
redis.call('DEL', indexKey)
redis.call('HSET', authKey,
  'generation_tag', ARGV[5],
  'generation_id', ARGV[3],
  'updated_ms', now_ms)
local bucket = '0'
if count > 10 then
  bucket = '11_100'
elseif count > 0 then
  bucket = '1_10'
end
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[6],
  'timestamp_ms', now_ms,
  'actor', 'startup',
  'operation', 'admin_secret_generation_changed',
  'target', 'admin_auth',
  'outcome', 'success',
  'reason', 'sessions_revoked_' .. bucket)
return {'rotated', count}
