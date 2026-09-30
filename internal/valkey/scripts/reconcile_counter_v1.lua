-- reconcile_counter_v1: set hr1:stats:queued_messages to a verified scanned
-- total, only if it still holds the value observed before the scan
-- (compare-and-set). The caller must quiesce writers for the entire scan;
-- numeric equality cannot detect accept/ack ABA changes. CAS is an extra
-- precondition, not a serving-time snapshot fence.
--
-- KEYS[1] queued counter hr1:stats:queued_messages
--
-- ARGV[1] observed value before the scan ("" when absent)
-- ARGV[2] verified total
--
-- Returns {"repaired"} | {"consistent"} | {"precondition_failed"} | {"wrong_type"}.

if #KEYS ~= 1 or #ARGV ~= 2 or not string.match(ARGV[2], '^[0-9]+$') then
  return redis.error_reply('ERR reconcile_counter_v1: invalid arguments')
end
local t = redis.call('TYPE', KEYS[1])['ok']
if t ~= 'none' and t ~= 'string' then
  return {'wrong_type'}
end
local current = redis.call('GET', KEYS[1]) or ''
if current ~= ARGV[1] then
  return {'precondition_failed'}
end
if current == ARGV[2] or (current == '' and ARGV[2] == '0') then
  return {'consistent'}
end
redis.call('SET', KEYS[1], ARGV[2])
return {'repaired'}
