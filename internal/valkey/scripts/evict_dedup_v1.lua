-- evict_dedup_v1: proactive deduplication early eviction for background
-- maintenance. While the live deduplication records (index members with
-- accepted_ms within the retention window) are at or above max_records,
-- remove the oldest live record and its index member, but only while that
-- record is at least min_retention_ms old, at most batch records per call.
-- Every candidate is validated before it is removed; an inconsistent
-- candidate stops the batch (never blind eviction) and is reported.
-- Valkey TIME is authoritative.
--
-- KEYS[1] dedup age index  hr1:dedup_age
--
-- ARGV[1] max_records
-- ARGV[2] min_retention_ms
-- ARGV[3] retention_ms (the live window)
-- ARGV[4] batch
-- ARGV[5] key prefix ("hr1")
--
-- Returns:
--   {"evicted", count, oldest_accepted_ms (0 when none live), live_records, stop}
--   {"wrong_type"}  the index is not a sorted set (no mutation)
-- stop is below_cap | min_retention | batch | candidate_invalid.

if #KEYS ~= 1 or #ARGV ~= 5 then
  return redis.error_reply('ERR evict_dedup_v1: expected 1 key and 5 arguments')
end
for i = 1, 4 do
  if not string.match(ARGV[i], '^[1-9][0-9]*$') or #ARGV[i] > 15 then
    return redis.error_reply('ERR evict_dedup_v1: argument ' .. i .. ' must be a positive integer')
  end
end
local prefix = ARGV[5]
if prefix == '' or KEYS[1] ~= prefix .. ':dedup_age' then
  return redis.error_reply('ERR evict_dedup_v1: keys do not match the prefix')
end
local dedupAge = KEYS[1]
local t = redis.call('TYPE', dedupAge)['ok']
if t ~= 'none' and t ~= 'zset' then
  return {'wrong_type'}
end

local maxRecords, minRetention = tonumber(ARGV[1]), tonumber(ARGV[2])
local retention, batch = tonumber(ARGV[3]), tonumber(ARGV[4])
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local liveMin = '(' .. (now_ms - retention)

local function valid_ms(value)
  return value and string.match(value, '^[1-9]%d*$') and #value <= 15
end

local count, stop = 0, 'batch'
local live = redis.call('ZCOUNT', dedupAge, liveMin, '+inf')
while count < batch do
  if live < maxRecords then
    stop = 'below_cap'
    break
  end
  local oldest = redis.call('ZRANGEBYSCORE', dedupAge, liveMin, '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
  local digest, score = oldest[1], tonumber(oldest[2])
  if not digest or not score or score ~= math.floor(score) then
    stop = 'candidate_invalid'
    break
  end
  if score > now_ms - minRetention then
    stop = 'min_retention'
    break
  end
  local record = prefix .. ':d:' .. digest
  if redis.call('TYPE', record)['ok'] ~= 'hash' then
    stop = 'candidate_invalid'
    break
  end
  local accepted = redis.call('HGET', record, 'accepted_ms')
  if not valid_ms(accepted) or tonumber(accepted) ~= score then
    stop = 'candidate_invalid'
    break
  end
  redis.call('DEL', record)
  redis.call('ZREM', dedupAge, digest)
  count = count + 1
  live = live - 1
end

local oldest = redis.call('ZRANGEBYSCORE', dedupAge, liveMin, '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
return {'evicted', count, tonumber(oldest[2]) or 0, live, stop}
