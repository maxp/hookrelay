-- operations_summary_v1: read one consistent snapshot of the operational
-- counters for the operations summary. Read-only: it never writes.
--
-- KEYS[1] queued counter    hr1:stats:queued_messages
-- KEYS[2] ready index       hr1:ready
-- KEYS[3] lease index       hr1:leases
-- KEYS[4] retry index       hr1:retries
-- KEYS[5] blocked index     hr1:blocked
-- KEYS[6] DLQ index         hr1:dlq
-- KEYS[7] endpoint listing  hr1:webhooks
-- KEYS[8] session index     hr1:admin_sessions
-- KEYS[9] audit stream      hr1:audit
--
-- ARGV[1] key prefix ("hr1")
--
-- Any key of the wrong type, or a queued counter that is not a
-- non-negative integer, is wrong_type. Absent keys count as empty; an
-- absent earliest/oldest/newest time is 0.
--
-- Returns:
--   {"snapshot", queued_messages, ready_recipients, leased_recipients,
--    retry_wait_recipients, blocked_recipients, earliest_lease_expires_ms,
--    earliest_retry_at_ms, dead_letters, oldest_dead_lettered_ms,
--    newest_dead_lettered_ms, webhook_endpoints, admin_sessions,
--    audit_length}
--   {"wrong_type"}

if #KEYS ~= 9 or #ARGV ~= 1 or ARGV[1] == '' then
  return redis.error_reply('ERR operations_summary_v1: expected 9 keys and 1 argument')
end
local prefix = ARGV[1]
local expected = {':stats:queued_messages', ':ready', ':leases', ':retries', ':blocked', ':dlq', ':webhooks', ':admin_sessions', ':audit'}
for i, suffix in ipairs(expected) do
  if KEYS[i] ~= prefix .. suffix then
    return redis.error_reply('ERR operations_summary_v1: keys do not match the prefix')
  end
end

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, want)
  local t = type_of(key)
  return t == 'none' or t == want
end

if not has_type(KEYS[1], 'string') or not has_type(KEYS[9], 'stream') then
  return {'wrong_type'}
end
for i = 2, 8 do
  if not has_type(KEYS[i], 'zset') then
    return {'wrong_type'}
  end
end

local queued = 0
local raw = redis.call('GET', KEYS[1])
if raw then
  if not string.match(raw, '^%d+$') or #raw > 15 then
    return {'wrong_type'}
  end
  queued = tonumber(raw)
end

-- The lowest (or, rev, highest) score of a sorted set; 0 when empty.
local function edge_score(key, rev)
  local r
  if rev then
    r = redis.call('ZRANGE', key, 0, 0, 'REV', 'WITHSCORES')
  else
    r = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  end
  if #r == 0 then
    return 0
  end
  return math.floor(tonumber(r[2]))
end

return {'snapshot',
  queued,
  redis.call('ZCARD', KEYS[2]),
  redis.call('ZCARD', KEYS[3]),
  redis.call('ZCARD', KEYS[4]),
  redis.call('ZCARD', KEYS[5]),
  edge_score(KEYS[3], false),
  edge_score(KEYS[4], false),
  redis.call('ZCARD', KEYS[6]),
  edge_score(KEYS[6], false),
  edge_score(KEYS[6], true),
  redis.call('ZCARD', KEYS[7]),
  redis.call('ZCARD', KEYS[8]),
  redis.call('XLEN', KEYS[9])}
