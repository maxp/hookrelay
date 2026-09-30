-- session_authenticate_v1: validate one administrative browser session by
-- its token digest and refresh its idle expiry at most once per refresh
-- interval. An expired, stale-generation, or malformed session is invalid
-- regardless of cleanup; the script deletes it and its index member so it
-- cannot be presented again (cleanup of session state, not a mutation of
-- an application resource). The absolute expiry never moves.
--
-- KEYS[1] generation record  hr1:admin_auth
-- KEYS[2] session index      hr1:admin_sessions
--
-- ARGV[1] session_digest (64 lowercase hex characters)
-- ARGV[2] idle_ms
-- ARGV[3] refresh_ms
-- ARGV[4] key prefix ("hr1")
--
-- Preconditions in order: the generation record, index, or session key of
-- the wrong type (wrong_type). Then: session absent (invalid absent; a
-- stale index member is removed); fields missing or malformed (invalid
-- malformed); now at or past either expiry (invalid expired); a
-- generation_id different from hr1:admin_auth, or no generation record
-- (invalid generation_changed). Every invalid session except an absent one
-- is deleted with its index member.
--
-- Writes (valid): when now - last_seen_ms >= refresh_ms, HSET last_seen_ms
-- and idle_expires_ms = min(now + idle_ms, absolute_expires_ms), and ZADD
-- the new score.
--
-- Returns:
--   {"valid", csrf_token, idle_expires_ms, absolute_expires_ms}
--   {"invalid", reason (absent | malformed | expired | generation_changed)}
--   {"wrong_type"}

if #KEYS ~= 2 or #ARGV ~= 4 then
  return redis.error_reply('ERR session_authenticate_v1: expected 2 keys and 4 arguments')
end
local digest, prefix = ARGV[1], ARGV[4]
if #digest ~= 64 or not string.match(digest, '^[0-9a-f]+$') then
  return redis.error_reply('ERR session_authenticate_v1: session_digest must be 64 lowercase hex characters')
end
for i = 2, 3 do
  if not string.match(ARGV[i], '^[1-9]%d*$') or #ARGV[i] > 12 then
    return redis.error_reply('ERR session_authenticate_v1: argument ' .. i .. ' must be a positive integer')
  end
end
if prefix == '' then
  return redis.error_reply('ERR session_authenticate_v1: argument 4 is empty')
end
local idleMs, refreshMs = tonumber(ARGV[2]), tonumber(ARGV[3])
if KEYS[1] ~= prefix .. ':admin_auth' or KEYS[2] ~= prefix .. ':admin_sessions' then
  return redis.error_reply('ERR session_authenticate_v1: keys do not match the prefix')
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

local function drop(reason)
  redis.call('DEL', sessionKey)
  redis.call('ZREM', indexKey, digest)
  return {'invalid', reason}
end

if type_of(sessionKey) == 'none' then
  redis.call('ZREM', indexKey, digest)
  return {'invalid', 'absent'}
end
local s = redis.call('HMGET', sessionKey, 'last_seen_ms', 'idle_expires_ms', 'absolute_expires_ms', 'generation_id', 'csrf_token')
local function valid_ms(v)
  return v and string.match(v, '^[1-9]%d*$') and #v <= 15
end
if not (valid_ms(s[1]) and valid_ms(s[2]) and valid_ms(s[3]) and s[4] and s[4] ~= '' and s[5]
    and #s[5] == 22 and string.match(s[5], '^[A-Za-z0-9_-]+$')) then
  return drop('malformed')
end
local lastSeen, idleExpires, absoluteExpires = tonumber(s[1]), tonumber(s[2]), tonumber(s[3])

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
if now_ms >= idleExpires or now_ms >= absoluteExpires then
  return drop('expired')
end
local generation = redis.call('HGET', authKey, 'generation_id')
if not generation or generation ~= s[4] then
  return drop('generation_changed')
end

if now_ms - lastSeen >= refreshMs then
  idleExpires = now_ms + idleMs
  if idleExpires > absoluteExpires then
    idleExpires = absoluteExpires
  end
  redis.call('HSET', sessionKey, 'last_seen_ms', now_ms, 'idle_expires_ms', idleExpires)
  redis.call('ZADD', indexKey, idleExpires, digest)
end
return {'valid', s[5], idleExpires, absoluteExpires}
