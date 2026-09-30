-- expire_sessions_v1: remove administrative browser sessions whose idle or
-- absolute expiry has passed, oldest first, in one bounded batch. The
-- index score is min(idle_expires_ms, absolute_expires_ms), so due members
-- are exactly the expired sessions. Expiry audit is best effort and
-- appended by the caller (UUIDv7 event identifiers are generated outside
-- Lua); an expired session is invalid whether or not this has run.
--
-- KEYS[1] session index  hr1:admin_sessions
--
-- ARGV[1] limit (1-1000)
-- ARGV[2] key prefix ("hr1")
--
-- Preconditions: the index of the wrong type (wrong_type).
--
-- Writes: DEL each due session Hash and ZREM its member.
--
-- Returns:
--   {"expired", removed_count, remaining_indexed}
--   {"wrong_type"}

if #KEYS ~= 1 or #ARGV ~= 2 then
  return redis.error_reply('ERR expire_sessions_v1: expected 1 key and 2 arguments')
end
if not string.match(ARGV[1], '^[1-9]%d*$') or tonumber(ARGV[1]) > 1000 then
  return redis.error_reply('ERR expire_sessions_v1: limit must be 1-1000')
end
local prefix = ARGV[2]
if prefix == '' or KEYS[1] ~= prefix .. ':admin_sessions' then
  return redis.error_reply('ERR expire_sessions_v1: keys do not match the prefix')
end
local indexKey = KEYS[1]
local t = redis.call('TYPE', indexKey)['ok']
if t ~= 'none' and t ~= 'zset' then
  return {'wrong_type'}
end

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local due = redis.call('ZRANGE', indexKey, '-inf', now_ms, 'BYSCORE', 'LIMIT', 0, tonumber(ARGV[1]))
for _, d in ipairs(due) do
  redis.call('DEL', prefix .. ':admin_session:' .. d)
  redis.call('ZREM', indexKey, d)
end
return {'expired', #due, redis.call('ZCARD', indexKey)}
