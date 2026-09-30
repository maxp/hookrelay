-- reconcile_session_v1: bring one administrative browser session and its
-- expiry/capacity index member into agreement. Sessions are disposable:
-- removing a suspicious one only forces a new login, so a malformed,
-- expired, or stale-generation session is deleted rather than held. The
-- session Hash is authoritative for a valid session; its member is
-- restored or rescored from it.
--
-- KEYS[1] generation record  hr1:admin_auth
-- KEYS[2] session index      hr1:admin_sessions
--
-- ARGV[1] session_digest (64 lowercase hex characters)
-- ARGV[2] key prefix ("hr1")
--
-- Preconditions: the generation record, index, or session key of the wrong
-- type (wrong_type; nothing is written).
--
-- Returns:
--   {"consistent"}
--   {"orphan_removed"}    member without a session Hash
--   {"removed", reason}   reason: malformed | expired | generation_changed
--   {"restored"}          member missing or with the wrong score
--   {"wrong_type"}

if #KEYS ~= 2 or #ARGV ~= 2 then
  return redis.error_reply('ERR reconcile_session_v1: expected 2 keys and 2 arguments')
end
local digest, prefix = ARGV[1], ARGV[2]
if #digest ~= 64 or not string.match(digest, '^[0-9a-f]+$') then
  return redis.error_reply('ERR reconcile_session_v1: session_digest must be 64 lowercase hex characters')
end
if prefix == '' or KEYS[1] ~= prefix .. ':admin_auth' or KEYS[2] ~= prefix .. ':admin_sessions' then
  return redis.error_reply('ERR reconcile_session_v1: keys do not match the prefix')
end
local authKey, indexKey = KEYS[1], KEYS[2]
local sessionKey = prefix .. ':admin_session:' .. digest

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, want)
  local t = type_of(key)
  return t == 'none' or t == want
end
if not (has_type(authKey, 'hash') and has_type(indexKey, 'zset') and has_type(sessionKey, 'hash')) then
  return {'wrong_type'}
end

local member = redis.call('ZSCORE', indexKey, digest)
if type_of(sessionKey) == 'none' then
  if member then
    redis.call('ZREM', indexKey, digest)
    return {'orphan_removed'}
  end
  return {'consistent'}
end

local function remove(reason)
  redis.call('DEL', sessionKey)
  redis.call('ZREM', indexKey, digest)
  return {'removed', reason}
end
local s = redis.call('HMGET', sessionKey, 'created_ms', 'last_seen_ms', 'idle_expires_ms', 'absolute_expires_ms', 'generation_id', 'csrf_token')
local function valid_ms(v)
  return v and string.match(v, '^[1-9]%d*$') and #v <= 15
end
if not (valid_ms(s[1]) and valid_ms(s[2]) and valid_ms(s[3]) and valid_ms(s[4]) and s[5] and s[5] ~= '' and s[6]
    and #s[6] == 22 and string.match(s[6], '^[A-Za-z0-9_-]+$')) then
  return remove('malformed')
end
local idleExpires, absoluteExpires = tonumber(s[3]), tonumber(s[4])
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
if now_ms >= idleExpires or now_ms >= absoluteExpires then
  return remove('expired')
end
local generation = redis.call('HGET', authKey, 'generation_id')
if not generation or generation ~= s[5] then
  return remove('generation_changed')
end
local score = idleExpires
if absoluteExpires < score then
  score = absoluteExpires
end
if not member or tonumber(member) ~= score then
  redis.call('ZADD', indexKey, score, digest)
  return {'restored'}
end
return {'consistent'}
