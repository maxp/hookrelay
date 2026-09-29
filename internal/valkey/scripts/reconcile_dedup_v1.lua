-- reconcile_dedup_v1: align a batch of deduplication records with the
-- hr1:dedup_age index. For each digest: a record past its expires_ms is
-- deleted with its index member; a live record missing from the index is
-- restored with its accepted_ms; an index member without a record is
-- removed. Only derived index state and already-expired records change.
--
-- KEYS[1] dedup age index hr1:dedup_age
--
-- ARGV[1]  key prefix ("hr1")
-- ARGV[2…] dedup identity digests
--
-- Returns {"reconciled", expired_removed, restored, orphans_removed, skipped}
-- or {"wrong_type"}. Malformed records are counted as skipped and hold readiness.

if #KEYS ~= 1 or #ARGV < 1 or ARGV[1] == '' then
  return redis.error_reply('ERR reconcile_dedup_v1: expected 1 key and a prefix')
end
local prefix, index = ARGV[1], KEYS[1]
if index ~= prefix .. ':dedup_age' then
  return redis.error_reply('ERR reconcile_dedup_v1: key does not match the prefix')
end
local it = redis.call('TYPE', index)['ok']
if it ~= 'none' and it ~= 'zset' then
  return {'wrong_type'}
end
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

local function valid_ms(value)
  return value and string.match(value, '^[1-9]%d*$') and #value <= 19
    and (#value < 19 or value <= '9223372036854775807')
end

local expired, restored, orphans, skipped = 0, 0, 0, 0
for i = 2, #ARGV do
  local digest = ARGV[i]
  local key = prefix .. ':d:' .. digest
  local t = redis.call('TYPE', key)['ok']
  if t == 'none' then
    orphans = orphans + redis.call('ZREM', index, digest)
  elseif t ~= 'hash' then
    skipped = skipped + 1
  else
    local rec = redis.call('HMGET', key, 'message_id', 'body_digest', 'accepted_ms', 'expires_ms')
    local accepted, expires = tonumber(rec[3]), tonumber(rec[4])
    if not rec[1] or rec[1] == '' or not rec[2] or rec[2] == ''
        or not valid_ms(rec[3]) or not valid_ms(rec[4])
        or not accepted or not expires or expires <= accepted then
      skipped = skipped + 1
    elseif expires <= now_ms then
      redis.call('DEL', key)
      redis.call('ZREM', index, digest)
      expired = expired + 1
    elseif not redis.call('ZSCORE', index, digest) then
      redis.call('ZADD', index, accepted, digest)
      restored = restored + 1
    end
  end
end
return {'reconciled', expired, restored, orphans, skipped}
