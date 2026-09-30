-- session_delete_v1: revoke one administrative browser session (logout).
-- Revocation has priority over audit: the logout event is appended in the
-- same operation only when a session existed, and the types are checked
-- first so the append cannot fail after the deletion.
--
-- KEYS[1] session index  hr1:admin_sessions
-- KEYS[2] audit stream   hr1:audit
--
-- ARGV[1] session_digest (64 lowercase hex characters)
-- ARGV[2] event_id
-- ARGV[3] request_id
-- ARGV[4] key prefix ("hr1")
--
-- Preconditions: the index, audit stream, or session key of the wrong type
-- (wrong_type).
--
-- Writes: DEL the session Hash, ZREM the index member; when the Hash
-- existed, XADD actor=admin_session, operation=admin_logout,
-- target=session.
--
-- Returns:
--   {"deleted"}
--   {"absent"}
--   {"wrong_type"}

if #KEYS ~= 2 or #ARGV ~= 4 then
  return redis.error_reply('ERR session_delete_v1: expected 2 keys and 4 arguments')
end
for i = 1, 4 do
  if ARGV[i] == '' then
    return redis.error_reply('ERR session_delete_v1: argument ' .. i .. ' is empty')
  end
end
local digest, prefix = ARGV[1], ARGV[4]
if #digest ~= 64 or not string.match(digest, '^[0-9a-f]+$') then
  return redis.error_reply('ERR session_delete_v1: session_digest must be 64 lowercase hex characters')
end
if KEYS[1] ~= prefix .. ':admin_sessions' or KEYS[2] ~= prefix .. ':audit' then
  return redis.error_reply('ERR session_delete_v1: keys do not match the prefix')
end
local indexKey, auditKey = KEYS[1], KEYS[2]
local sessionKey = prefix .. ':admin_session:' .. digest

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, want)
  local t = type_of(key)
  return t == 'none' or t == want
end
if not (has_type(indexKey, 'zset') and has_type(auditKey, 'stream') and has_type(sessionKey, 'hash')) then
  return {'wrong_type'}
end

local existed = redis.call('DEL', sessionKey) == 1
redis.call('ZREM', indexKey, digest)
if not existed then
  return {'absent'}
end
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[2],
  'timestamp_ms', now_ms,
  'actor', 'admin_session',
  'operation', 'admin_logout',
  'target', 'session',
  'request_id', ARGV[3],
  'outcome', 'success')
return {'deleted'}
