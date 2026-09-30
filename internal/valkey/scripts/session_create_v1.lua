-- session_create_v1: create one administrative browser session after a
-- successful Admin Secret comparison, and append the mandatory login audit
-- event in the same operation. Expired sessions are removed first so they
-- never hold capacity; the caller appends their best-effort expiry audit
-- (event identifiers are UUIDv7, generated outside Lua). An existing
-- session is never evicted. The session token itself is never stored:
-- the caller passes its SHA-256 digest.
--
-- KEYS[1] generation record  hr1:admin_auth
-- KEYS[2] session index      hr1:admin_sessions
-- KEYS[3] audit stream       hr1:audit
--
-- ARGV[1] session_digest (64 lowercase hex characters)
-- ARGV[2] csrf_token (22 base64url characters)
-- ARGV[3] idle_ms
-- ARGV[4] absolute_ms
-- ARGV[5] capacity
-- ARGV[6] event_id
-- ARGV[7] request_id
-- ARGV[8] key prefix ("hr1")
--
-- Preconditions in order: any key of the wrong type, including the new
-- session key (wrong_type); hr1:admin_auth absent or without a
-- generation_id (auth_uninitialized); the session key already present
-- (collision). Then expired index members are removed (at most 1000);
-- with capacity or more indexed sessions left, capacity_exceeded (the
-- cleanup stays; nothing else is written).
--
-- Writes: DEL each expired session Hash and ZREM it; HSET the session
-- (created_ms, last_seen_ms, idle_expires_ms, absolute_expires_ms,
-- generation_id, csrf_token); ZADD score=idle_expires_ms; XADD
-- actor=admin_session, operation=admin_login, target=session.
--
-- Returns:
--   {"created", created_ms, idle_expires_ms, absolute_expires_ms, expired_count}
--   {"capacity_exceeded", expired_count}
--   {"auth_uninitialized"}
--   {"collision"}
--   {"wrong_type"}

if #KEYS ~= 3 or #ARGV ~= 8 then
  return redis.error_reply('ERR session_create_v1: expected 3 keys and 8 arguments')
end
for i = 1, 8 do
  if ARGV[i] == '' then
    return redis.error_reply('ERR session_create_v1: argument ' .. i .. ' is empty')
  end
end
local digest, csrf, prefix = ARGV[1], ARGV[2], ARGV[8]
if #digest ~= 64 or not string.match(digest, '^[0-9a-f]+$') then
  return redis.error_reply('ERR session_create_v1: session_digest must be 64 lowercase hex characters')
end
if #csrf ~= 22 or not string.match(csrf, '^[A-Za-z0-9_-]+$') then
  return redis.error_reply('ERR session_create_v1: csrf_token must be 22 base64url characters')
end
for i = 3, 5 do
  if not string.match(ARGV[i], '^[1-9]%d*$') or #ARGV[i] > 12 then
    return redis.error_reply('ERR session_create_v1: argument ' .. i .. ' must be a positive integer')
  end
end
local idleMs, absoluteMs, capacity = tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
if KEYS[1] ~= prefix .. ':admin_auth' or KEYS[2] ~= prefix .. ':admin_sessions' or KEYS[3] ~= prefix .. ':audit' then
  return redis.error_reply('ERR session_create_v1: keys do not match the prefix')
end
local authKey, indexKey, auditKey = KEYS[1], KEYS[2], KEYS[3]
local sessionPrefix = prefix .. ':admin_session:'
local sessionKey = sessionPrefix .. digest

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, want)
  local t = type_of(key)
  return t == 'none' or t == want
end
if not (has_type(authKey, 'hash') and has_type(indexKey, 'zset') and has_type(auditKey, 'stream')
    and has_type(sessionKey, 'hash')) then
  return {'wrong_type'}
end
local generation = redis.call('HGET', authKey, 'generation_id')
if not generation or generation == '' then
  return {'auth_uninitialized'}
end
if type_of(sessionKey) ~= 'none' then
  return {'collision'}
end

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

-- Expired sessions never hold capacity.
local expired = redis.call('ZRANGE', indexKey, '-inf', now_ms, 'BYSCORE', 'LIMIT', 0, 1000)
for _, d in ipairs(expired) do
  redis.call('DEL', sessionPrefix .. d)
  redis.call('ZREM', indexKey, d)
end
if redis.call('ZCARD', indexKey) >= capacity then
  return {'capacity_exceeded', #expired}
end

local idleExpires = now_ms + idleMs
local absoluteExpires = now_ms + absoluteMs
if idleExpires > absoluteExpires then
  idleExpires = absoluteExpires
end
redis.call('HSET', sessionKey,
  'created_ms', now_ms,
  'last_seen_ms', now_ms,
  'idle_expires_ms', idleExpires,
  'absolute_expires_ms', absoluteExpires,
  'generation_id', generation,
  'csrf_token', csrf)
redis.call('ZADD', indexKey, idleExpires, digest)
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[6],
  'timestamp_ms', now_ms,
  'actor', 'admin_session',
  'operation', 'admin_login',
  'target', 'session',
  'request_id', ARGV[7],
  'outcome', 'success')
return {'created', now_ms, idleExpires, absoluteExpires, #expired}
