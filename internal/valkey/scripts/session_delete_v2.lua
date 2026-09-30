-- session_delete_v2: revoke one administrative browser session (logout).
-- Revocation has priority over audit. This operation atomically removes the
-- session Hash and its index member; the caller appends the best-effort logout
-- audit after confirmed deletion.
--
-- KEYS[1] session index  hr1:admin_sessions
--
-- ARGV[1] session_digest (64 lowercase hex characters)
-- ARGV[2] key prefix ("hr1")
--
-- Preconditions: the index or session key of the wrong type (wrong_type).
--
-- Writes: DEL the session Hash and ZREM the index member.
--
-- Returns:
--   {"deleted"}
--   {"absent"}
--   {"wrong_type"}

if #KEYS ~= 1 or #ARGV ~= 2 then
  return redis.error_reply('ERR session_delete_v2: expected 1 key and 2 arguments')
end
for i = 1, 2 do
  if ARGV[i] == '' then
    return redis.error_reply('ERR session_delete_v2: argument ' .. i .. ' is empty')
  end
end
local digest, prefix = ARGV[1], ARGV[2]
if #digest ~= 64 or not string.match(digest, '^[0-9a-f]+$') then
  return redis.error_reply('ERR session_delete_v2: session_digest must be 64 lowercase hex characters')
end
if KEYS[1] ~= prefix .. ':admin_sessions' then
  return redis.error_reply('ERR session_delete_v2: key does not match the prefix')
end
local indexKey = KEYS[1]
local sessionKey = prefix .. ':admin_session:' .. digest

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, want)
  local t = type_of(key)
  return t == 'none' or t == want
end
if not (has_type(indexKey, 'zset') and has_type(sessionKey, 'hash')) then
  return {'wrong_type'}
end

local existed = redis.call('DEL', sessionKey) == 1
redis.call('ZREM', indexKey, digest)
if not existed then
  return {'absent'}
end
return {'deleted'}
