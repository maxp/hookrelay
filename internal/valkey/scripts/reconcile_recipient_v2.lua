-- reconcile_recipient_v2: verify one Recipient's authoritative state and
-- atomically repair only derived structures (ready, leases, retries, and
-- blocked index memberships) or isolate ambiguous state behind a block
-- marker. reconcile_recipient_v1 plus: retry_wait is a valid head status
-- (retries membership repaired to retry_at_ms), isolating a Recipient also
-- removes its retries member, and a due retry is reported as due_retry like
-- a due lease. Due work is never executed here: the caller runs the
-- registered expiry/activation transitions before readiness.
-- Authoritative state (queue, head state, messages) is never rewritten.
-- Per-recipient keys are resolved inside the script (standalone Valkey).
--
-- KEYS[1] ready index     hr1:ready
-- KEYS[2] ready sequence  hr1:ready_seq
-- KEYS[3] lease index     hr1:leases
-- KEYS[4] blocked index   hr1:blocked
-- KEYS[5] retry index     hr1:retries
--
-- ARGV[1] recipient_identity
-- ARGV[2] message check bound (per-recipient queue limit)
-- ARGV[3] key prefix ("hr1")
--
-- Returns {status, repairs, queue_length, reason} (queue_length -1 when the
-- queue key has an unexpected type):
--   consistent      nothing to do
--   repaired        derived memberships repaired (repairs > 0)
--   blocked         a new block marker was created with reason
--   already_blocked an existing marker was kept; derived entries aligned
--   drained         no queue and no state: stale memberships removed
--   due_lease       a leased head is past its deadline: its head state is
--                   untouched; only its derived locator is repaired (the
--                   leases member at the deadline, no ready/retries/blocked
--                   member) so maintenance can still find it
--   due_retry       a retry_wait head is past retry_at_ms: likewise, with
--                   the retries member at retry_at_ms
--   unhandled       state that cannot be isolated safely (e.g. a marker
--                   key of the wrong type): no mutation

if #KEYS ~= 5 or #ARGV ~= 3 then
  return redis.error_reply('ERR reconcile_recipient_v2: expected 5 keys and 3 arguments')
end
if ARGV[1] == '' or not string.match(ARGV[2], '^[1-9][0-9]*$') or ARGV[3] == '' then
  return redis.error_reply('ERR reconcile_recipient_v2: invalid arguments')
end
local prefix, rid = ARGV[3], ARGV[1]
if KEYS[1] ~= prefix .. ':ready' or KEYS[2] ~= prefix .. ':ready_seq' or KEYS[3] ~= prefix .. ':leases' or KEYS[4] ~= prefix .. ':blocked'
    or KEYS[5] ~= prefix .. ':retries' then
  return redis.error_reply('ERR reconcile_recipient_v2: keys do not match the prefix')
end
local readyKey, readySeqKey, leasesKey, blockedKey, retriesKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local queueKey = prefix .. ':r:' .. rid .. ':q'
local stateKey = prefix .. ':r:' .. rid .. ':s'
local markerKey = prefix .. ':q:' .. rid

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
for _, k in ipairs({readyKey, leasesKey, blockedKey, retriesKey}) do
  local t = type_of(k)
  if t ~= 'none' and t ~= 'zset' then
    return {'unhandled', 0, 0, 'global_index_type'}
  end
end
local st = type_of(readySeqKey)
if st ~= 'none' and st ~= 'string' then
  return {'unhandled', 0, 0, 'global_index_type'}
end
-- A repair may allocate a fairness sequence. Reject invalid or exhausted
-- counters before any derived membership is changed.
local seq = redis.call('GET', readySeqKey)
if seq and (not (seq == '0' or string.match(seq, '^[1-9]%d*$')) or #seq > 19
    or (#seq == 19 and seq >= '9223372036854775807')) then
  return {'unhandled', 0, 0, 'global_index_value'}
end

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local qt, stt, mt = type_of(queueKey), type_of(stateKey), type_of(markerKey)
-- queue_length is -1 when the queue key exists with an unexpected type and
-- cannot be counted.
local queueLen = 0
if qt == 'list' then
  queueLen = redis.call('LLEN', queueKey)
elseif qt ~= 'none' then
  queueLen = -1
end

local function member(key)
  return redis.call('ZSCORE', key, rid)
end

-- Aligns derived entries for a blocked Recipient; returns repair count.
local function align_blocked(detected)
  local repairs = 0
  if not member(blockedKey) then
    redis.call('ZADD', blockedKey, detected, rid)
    repairs = repairs + 1
  end
  repairs = repairs + redis.call('ZREM', readyKey, rid) + redis.call('ZREM', leasesKey, rid)
    + redis.call('ZREM', retriesKey, rid)
  return repairs
end

local function block(reason)
  redis.call('HSET', markerKey, 'detected_ms', now_ms, 'reason_code', reason)
  align_blocked(now_ms)
  return {'blocked', 0, queueLen, reason}
end

-- 1. An existing marker stays; derived entries follow it.
if mt ~= 'none' then
  if mt ~= 'hash' then
    return {'unhandled', 0, queueLen, 'marker_type'}
  end
  local detected = tonumber(redis.call('HGET', markerKey, 'detected_ms')) or now_ms
  local reason = redis.call('HGET', markerKey, 'reason_code') or ''
  return {'already_blocked', align_blocked(detected), queueLen, reason}
end

-- 2. Key types.
if (qt ~= 'none' and qt ~= 'list') or (stt ~= 'none' and stt ~= 'hash') then
  return block('unsupported_key_type')
end

-- 3. Drained: no queue and no state. Remove stale memberships.
if qt == 'none' and stt == 'none' then
  local removed = redis.call('ZREM', readyKey, rid) + redis.call('ZREM', leasesKey, rid) + redis.call('ZREM', blockedKey, rid)
    + redis.call('ZREM', retriesKey, rid)
  if removed > 0 then
    return {'drained', removed, 0, ''}
  end
  return {'consistent', 0, 0, ''}
end
if qt == 'none' then
  -- A head state without a queue does not match any queue head.
  return block('queue_head_mismatch')
end
if stt == 'none' then
  return block('head_state_missing')
end

-- 4. Head invariant and head state completeness.
local state = redis.call('HMGET', stateKey, 'status', 'head_message_id', 'delivery_cycle', 'attempt', 'lease_expires_ms', 'delivery_token', 'retry_at_ms')
local head = redis.call('LINDEX', queueKey, 0)
if state[2] ~= head then
  return block('queue_head_mismatch')
end
if not (state[3] and state[4]) or (state[1] ~= 'ready' and state[1] ~= 'leased' and state[1] ~= 'retry_wait') then
  return block('head_state_missing')
end

-- 5. Queued message blobs within the per-recipient bound.
local ids = redis.call('LRANGE', queueKey, 0, tonumber(ARGV[2]) - 1)
for _, id in ipairs(ids) do
  if type_of(prefix .. ':m:' .. id) ~= 'string' then
    return block('head_message_missing')
  end
end

-- 6. Due work: the caller runs the transition. Only the derived locator is
-- repaired, so a failed startup transition stays visible to maintenance.
local function locate(index, score, others)
  local repairs = 0
  if tonumber(member(index)) ~= score then
    redis.call('ZADD', index, score, rid)
    repairs = 1
  end
  for _, k in ipairs(others) do
    repairs = repairs + redis.call('ZREM', k, rid)
  end
  return repairs
end
local lease = tonumber(state[5])
if state[1] == 'leased' then
  if not lease or not state[6] then
    return block('head_state_missing')
  end
  if lease <= now_ms then
    return {'due_lease', locate(leasesKey, lease, {readyKey, retriesKey, blockedKey}), queueLen, ''}
  end
end
local retryAt = nil
if state[1] == 'retry_wait' then
  if not (state[7] and string.match(state[7], '^[1-9]%d*$') and #state[7] <= 15) then
    return block('head_state_missing')
  end
  retryAt = tonumber(state[7])
  if retryAt <= now_ms then
    return {'due_retry', locate(retriesKey, retryAt, {readyKey, leasesKey, blockedKey}), queueLen, ''}
  end
end

-- 7. Derived memberships per status.
local repairs = redis.call('ZREM', blockedKey, rid)
if state[1] == 'leased' then
  if tonumber(member(leasesKey)) ~= lease then
    redis.call('ZADD', leasesKey, lease, rid)
    repairs = repairs + 1
  end
  repairs = repairs + redis.call('ZREM', readyKey, rid) + redis.call('ZREM', retriesKey, rid)
elseif state[1] == 'retry_wait' then
  if tonumber(member(retriesKey)) ~= retryAt then
    redis.call('ZADD', retriesKey, retryAt, rid)
    repairs = repairs + 1
  end
  repairs = repairs + redis.call('ZREM', readyKey, rid) + redis.call('ZREM', leasesKey, rid)
else
  if not member(readyKey) then
    local seq = redis.call('INCR', readySeqKey)
    redis.call('ZADD', readyKey, seq, rid)
    repairs = repairs + 1
  end
  repairs = repairs + redis.call('ZREM', leasesKey, rid) + redis.call('ZREM', retriesKey, rid)
end
if repairs > 0 then
  return {'repaired', repairs, queueLen, ''}
end
return {'consistent', 0, queueLen, ''}
