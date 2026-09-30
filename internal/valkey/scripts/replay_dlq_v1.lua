-- replay_dlq_v1: return one Dead-letter Message to its Recipient's queue
-- with a new Delivery Cycle (attempt 1), ahead of every not-yet-started
-- message, and append the mandatory audit event in the same operation.
-- An already leased or retry-wait head is never interrupted: the replayed
-- message is inserted immediately after it (after_active_head) and its new
-- cycle/attempt is saved as pending state in its hr1:mi metadata until it
-- becomes head. Otherwise it becomes the head (head); a preempted ready
-- head first saves its own cycle/attempt as pending state (creating the
-- metadata key for a message accepted before the key existed), so a ready
-- retry keeps its attempt. No other queued message's pending state is
-- touched. The original Deduplication Identity is checked: a record that
-- points to another message refuses the default replay, and keep_current
-- replays while leaving that newer mapping unchanged (replay never
-- repoints it). The blob, metadata, and attempt history are kept; history
-- beyond the latest 10 Delivery Cycles (counting the new one) is folded
-- into the leading archived_cycles_summary exactly as nack_v3 does.
-- Per-recipient keys are resolved inside the script (standalone Valkey is
-- a precondition). Every precondition is checked before the first write;
-- Valkey TIME is authoritative for replayed_ms.
--
-- KEYS[1] DLQ index       hr1:dlq
-- KEYS[2] ready index     hr1:ready
-- KEYS[3] ready sequence  hr1:ready_seq
-- KEYS[4] retry index     hr1:retries
-- KEYS[5] lease index     hr1:leases
-- KEYS[6] blocked index   hr1:blocked
-- KEYS[7] queued counter  hr1:stats:queued_messages
-- KEYS[8] audit stream    hr1:audit
--
-- ARGV[1] message_id
-- ARGV[2] resolution: reject | keep_current
-- ARGV[3] event_id
-- ARGV[4] request_id
-- ARGV[5] key prefix ("hr1")
--
-- Preconditions in order: hr1:dl absent (not_found) or not a Hash, or
-- without a recipient_identity and valid delivery_cycle (wrong_type); blob
-- absent (message_missing) or not a String (wrong_type); block marker
-- present (recipient_blocked); a dedup record for a nonempty
-- dedup_identity_digest that points to another message
-- (deduplication_conflict unless keep_current; a dedup record that is not
-- a Hash is wrong_type); then wrong_type for: any key of the wrong type; a
-- queue and head state that disagree (state without queue or queue without
-- state, head_message_id not the queue head, unknown status, malformed
-- cycle/attempt); the message already queued; pending fields already on
-- the replayed message or on a preempted ready head; a malformed history
-- entry; a queued counter that is not a non-negative integer; a ready
-- sequence that cannot be incremented.
--
-- Writes: pending state and LINSERT after the head, or pending state of
-- the preempted ready head, LPUSH, fresh ready head state and ready
-- membership with a fresh sequence; history fold; DEL hr1:dl; ZREM dlq;
-- INCR queued counter; XADD audit operation=dead_letter_replayed,
-- target=<message_id>, reason=<deduplication_resolution>.
--
-- Returns:
--   {"replayed", delivery_cycle, queue_position (head | after_active_head),
--    replayed_ms, deduplication_resolution (not_conflicting | kept_current),
--    recipient_identity}
--   {"not_found"}
--   {"message_missing"}
--   {"recipient_blocked"}
--   {"deduplication_conflict"}
--   {"wrong_type"}

if #KEYS ~= 8 or #ARGV ~= 5 then
  return redis.error_reply('ERR replay_dlq_v1: expected 8 keys and 5 arguments')
end
for i = 1, 5 do
  if ARGV[i] == '' then
    return redis.error_reply('ERR replay_dlq_v1: argument ' .. i .. ' is empty')
  end
end
if ARGV[2] ~= 'reject' and ARGV[2] ~= 'keep_current' then
  return redis.error_reply('ERR replay_dlq_v1: resolution must be reject or keep_current')
end
local messageID, prefix = ARGV[1], ARGV[5]
if KEYS[1] ~= prefix .. ':dlq' or KEYS[2] ~= prefix .. ':ready' or KEYS[3] ~= prefix .. ':ready_seq'
    or KEYS[4] ~= prefix .. ':retries' or KEYS[5] ~= prefix .. ':leases' or KEYS[6] ~= prefix .. ':blocked'
    or KEYS[7] ~= prefix .. ':stats:queued_messages' or KEYS[8] ~= prefix .. ':audit' then
  return redis.error_reply('ERR replay_dlq_v1: keys do not match the prefix')
end
local dlqKey, readyKey, readySeqKey, retriesKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local leasesKey, blockedKey, counterKey, auditKey = KEYS[5], KEYS[6], KEYS[7], KEYS[8]
local dlKey = prefix .. ':dl:' .. messageID
local blobKey = prefix .. ':m:' .. messageID
local metaKey = prefix .. ':mi:' .. messageID
local historyKey = prefix .. ':a:' .. messageID

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, expected)
  local t = type_of(key)
  return t == 'none' or t == expected
end
local function valid_ms(value)
  return value and string.match(value, '^[1-9]%d*$') and #value <= 15
end

-- 1. The dead-letter record.
local dt = type_of(dlKey)
if dt == 'none' then
  return {'not_found'}
end
if dt ~= 'hash' then
  return {'wrong_type'}
end
local dl = redis.call('HMGET', dlKey, 'recipient_identity', 'delivery_cycle', 'dedup_identity_digest')
local rid = dl[1]
if not rid or rid == '' or not valid_ms(dl[2]) then
  return {'wrong_type'}
end
local queueKey = prefix .. ':r:' .. rid .. ':q'
local stateKey = prefix .. ':r:' .. rid .. ':s'
local markerKey = prefix .. ':q:' .. rid

-- 2. The Canonical Message must still exist.
local bt = type_of(blobKey)
if bt == 'none' then
  return {'message_missing'}
end
if bt ~= 'string' then
  return {'wrong_type'}
end

-- 3. A blocked Recipient is not replayed into.
if type_of(markerKey) ~= 'none' then
  return {'recipient_blocked'}
end

-- 4. The original deduplication mapping.
local resolution = 'not_conflicting'
local digest = dl[3]
if digest and digest ~= '' then
  local dedupKey = prefix .. ':d:' .. digest
  if not has_type(dedupKey, 'hash') then
    return {'wrong_type'}
  end
  local current = redis.call('HGET', dedupKey, 'message_id')
  if current and current ~= messageID then
    if ARGV[2] ~= 'keep_current' then
      return {'deduplication_conflict'}
    end
    resolution = 'kept_current'
  end
end

-- 5. Key types.
if not (has_type(dlqKey, 'zset') and has_type(readyKey, 'zset') and has_type(readySeqKey, 'string')
    and has_type(retriesKey, 'zset') and has_type(leasesKey, 'zset') and has_type(blockedKey, 'zset')
    and has_type(counterKey, 'string') and has_type(auditKey, 'stream')
    and has_type(queueKey, 'list') and has_type(stateKey, 'hash') and has_type(metaKey, 'hash')
    and has_type(historyKey, 'list')) then
  return {'wrong_type'}
end

-- 6. Queue and head state must agree; the message must not be queued.
local qlen = redis.call('LLEN', queueKey)
local st = type_of(stateKey)
local state = nil
if qlen == 0 then
  if st ~= 'none' then
    return {'wrong_type'}
  end
else
  if st == 'none' then
    return {'wrong_type'}
  end
  state = redis.call('HMGET', stateKey, 'status', 'head_message_id', 'delivery_cycle', 'attempt')
  if state[2] ~= redis.call('LINDEX', queueKey, 0)
      or (state[1] ~= 'ready' and state[1] ~= 'leased' and state[1] ~= 'retry_wait')
      or not (valid_ms(state[3]) and valid_ms(state[4])) then
    return {'wrong_type'}
  end
  if redis.call('LPOS', queueKey, messageID) then
    return {'wrong_type'}
  end
end
local behindActive = state and (state[1] == 'leased' or state[1] == 'retry_wait')
local headMetaKey = state and state[1] == 'ready' and (prefix .. ':mi:' .. state[2]) or nil

-- 7. Pending state: never overwrite a saved pair.
local function has_pending(key)
  if type_of(key) == 'none' then
    return false
  end
  return redis.call('HEXISTS', key, 'pending_delivery_cycle') == 1
    or redis.call('HEXISTS', key, 'pending_attempt') == 1
end
if has_pending(metaKey) then
  return {'wrong_type'}
end
if headMetaKey then
  if not has_type(headMetaKey, 'hash') or has_pending(headMetaKey) then
    return {'wrong_type'}
  end
end

-- 8. Fallible numeric writes.
local queued = redis.call('GET', counterKey)
if queued and (not (queued == '0' or string.match(queued, '^[1-9]%d*$')) or #queued > 19
    or (#queued == 19 and queued >= '9223372036854775807')) then
  return {'wrong_type'}
end
if not behindActive then
  local seq = redis.call('GET', readySeqKey)
  if seq and (not (seq == '0' or string.match(seq, '^[1-9]%d*$')) or #seq > 19
      or (#seq == 19 and seq >= '9223372036854775807')) then
    return {'wrong_type'}
  end
end

-- 9. Parse the retained history before any write.
local function integer(v)
  return type(v) == 'number' and v >= 0 and v == math.floor(v)
end
local entries = {}
for i, raw in ipairs(redis.call('LRANGE', historyKey, 0, -1)) do
  local ok, e = pcall(cjson.decode, raw)
  if not ok or type(e) ~= 'table' then
    return {'wrong_type'}
  end
  if e['kind'] == 'attempt' then
    if not (integer(e['delivery_cycle']) and integer(e['attempt']) and integer(e['claimed_ms'])
        and integer(e['lease_expires_ms']) and integer(e['completed_ms'])) then
      return {'wrong_type'}
    end
  elseif e['kind'] == 'archived_cycles_summary' and i == 1 then
    if not (integer(e['archived_cycles']) and integer(e['archived_attempts'])
        and integer(e['first_archived_ms']) and integer(e['last_archived_ms'])) then
      return {'wrong_type'}
    end
  else
    return {'wrong_type'}
  end
  entries[#entries + 1] = {raw = raw, e = e}
end

-- Writes.
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local cycle = tonumber(dl[2]) + 1
local position
if behindActive then
  redis.call('HSET', metaKey, 'pending_delivery_cycle', cycle, 'pending_attempt', 1)
  redis.call('LINSERT', queueKey, 'AFTER', state[2], messageID)
  position = 'after_active_head'
else
  if headMetaKey then
    redis.call('HSET', headMetaKey, 'pending_delivery_cycle', state[3], 'pending_attempt', state[4])
  end
  redis.call('LPUSH', queueKey, messageID)
  redis.call('DEL', stateKey)
  redis.call('HSET', stateKey,
    'status', 'ready',
    'head_message_id', messageID,
    'delivery_cycle', cycle,
    'attempt', 1)
  redis.call('ZADD', readyKey, redis.call('INCR', readySeqKey), rid)
  position = 'head'
end

-- Keep the latest 10 Delivery Cycles, counting the new one.
local cycles, seen = {}, {}
for _, x in ipairs(entries) do
  local c = x.e['delivery_cycle']
  if x.e['kind'] == 'attempt' and not seen[c] then
    seen[c] = true
    cycles[#cycles + 1] = c
  end
end
if #cycles + 1 > 10 then
  table.sort(cycles)
  local archived = {}
  for i = 1, #cycles + 1 - 10 do
    archived[cycles[i]] = true
  end
  local sum = {cycles = 0, attempts = 0}
  local kept = {}
  for _, x in ipairs(entries) do
    local e = x.e
    if e['kind'] == 'archived_cycles_summary' then
      sum.cycles, sum.attempts = e['archived_cycles'], e['archived_attempts']
      sum.first, sum.last = e['first_archived_ms'], e['last_archived_ms']
    elseif archived[e['delivery_cycle']] then
      sum.attempts = sum.attempts + 1
      if not sum.first or e['claimed_ms'] < sum.first then
        sum.first = e['claimed_ms']
      end
      if not sum.last or e['completed_ms'] > sum.last then
        sum.last = e['completed_ms']
      end
    else
      kept[#kept + 1] = x.raw
    end
  end
  sum.cycles = sum.cycles + (#cycles + 1 - 10)
  redis.call('DEL', historyKey)
  redis.call('RPUSH', historyKey, string.format(
    '{"kind":"archived_cycles_summary","archived_cycles":%d,"archived_attempts":%d,"first_archived_ms":%d,"last_archived_ms":%d}',
    sum.cycles, sum.attempts, sum.first, sum.last))
  if #kept > 0 then
    redis.call('RPUSH', historyKey, unpack(kept))
  end
end

redis.call('DEL', dlKey)
redis.call('ZREM', dlqKey, messageID)
redis.call('INCR', counterKey)
redis.call('XADD', auditKey, 'MAXLEN', '~', '1000000', '*',
  'event_id', ARGV[3],
  'timestamp_ms', now_ms,
  'actor', 'admin_bearer',
  'operation', 'dead_letter_replayed',
  'target', messageID,
  'request_id', ARGV[4],
  'outcome', 'success',
  'reason', resolution)

return {'replayed', cycle, position, now_ms, resolution, rid}
