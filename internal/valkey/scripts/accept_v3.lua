-- accept_v3: atomically accept one Canonical Message or prove it duplicate.
-- accept_v2 plus bounded acceptance-time deduplication early eviction: when
-- the live deduplication index is full, the oldest live record is evicted
-- (record and index member) in the same operation, but only if it is valid
-- and at least min_retention_ms old; otherwise dedup_capacity with no
-- mutation. The accepted tuple adds early_evicted (0 or 1). accept_v2 added
-- the message metadata Hash hr1:mi:<message_id>, which keeps the
-- Deduplication Identity for dead-letter replay conflict checks (the
-- Canonical Message blob is the public Consumer API message and carries no
-- such field). Creates the deduplication record (with TTL and age-index
-- member), stores the message blob and its metadata, appends it to the
-- Recipient queue, creates the head state and ready-index membership when
-- the queue becomes non-empty, and increments the global queued-message
-- counter — all or nothing. Every
-- precondition is checked before the first write; Valkey TIME is
-- authoritative for accepted_ms and the dedup expiry.
--
-- KEYS[1]  dedup record     hr1:d:<dedup_identity_digest>
-- KEYS[2]  dedup age index  hr1:dedup_age
-- KEYS[3]  message blob     hr1:m:<message_id>
-- KEYS[4]  queue            hr1:r:<recipient_identity>:q
-- KEYS[5]  head state       hr1:r:<recipient_identity>:s
-- KEYS[6]  ready index      hr1:ready
-- KEYS[7]  ready sequence   hr1:ready_seq
-- KEYS[8]  lease index      hr1:leases
-- KEYS[9]  blocked index    hr1:blocked
-- KEYS[10] block marker     hr1:q:<recipient_identity>
-- KEYS[11] queued counter   hr1:stats:queued_messages
-- KEYS[12] message metadata hr1:mi:<message_id>
--
-- ARGV[1]  message_id (UUIDv7, caller-generated candidate)
-- ARGV[2]  dedup_identity_digest (lowercase hex SHA-256)
-- ARGV[3]  body_digest (lowercase hex SHA-256 of the exact request body)
-- ARGV[4]  received_ms
-- ARGV[5]  occurred_ms (empty when absent)
-- ARGV[6]  message_json (compact Canonical Message)
-- ARGV[7]  recipient_identity
-- ARGV[8]  max_queued_messages_global
-- ARGV[9]  max_queued_messages_per_recipient
-- ARGV[10] max_dedup_records
-- ARGV[11] dedup_retention_ms
-- ARGV[12] dedup_min_retention_ms (never evict a younger record early)
--
-- Precondition order: block marker, dedup record, key types, head-state
-- consistency, capacity. Key types are checked before capacity because
-- LLEN/GET/ZCOUNT on an unexpected type would raise a runtime error.
-- The dedup capacity counts only live index members (accepted_ms within the
-- retention window); expired members are pruned in bounded batches after a
-- successful acceptance, never on a refusal path.
--
-- Returns:
--   {"accepted", accepted_ms, early_evicted (0 | 1)}
--   {"duplicate", original_message_id}            same body digest
--   {"duplicate_conflict", original_message_id}   different body digest
--   {"recipient_blocked"}                         block marker present
--   {"recipient_capacity"}                        per-recipient queue full
--   {"global_capacity"}                           global queue full
--   {"dedup_capacity"}                            dedup cap reached and the
--                                                 oldest record is too young
--   {"wrong_type"}                                a key has an unexpected type,
--                                                 or the eviction candidate's
--                                                 record is inconsistent
--   {"state_inconsistent"}                        empty queue with a head state
-- A message_id that is already stored (blob or metadata) is a caller bug and
-- an error reply.

-- Argument validation. A violation is a caller bug, reported as a script
-- error rather than a bounded status.
if #KEYS ~= 12 or #ARGV ~= 12 then
  return redis.error_reply('ERR accept_v3: expected 12 keys and 12 arguments')
end
for _, i in ipairs({1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12}) do
  if ARGV[i] == '' then
    return redis.error_reply('ERR accept_v3: argument ' .. i .. ' is empty')
  end
end
local function positive_integer(s)
  return string.match(s, '^[1-9][0-9]*$') ~= nil
end
for _, i in ipairs({4, 8, 9, 10, 11, 12}) do
  if not positive_integer(ARGV[i]) then
    return redis.error_reply('ERR accept_v3: argument ' .. i .. ' must be a positive integer')
  end
end
if ARGV[5] ~= '' and not positive_integer(ARGV[5]) then
  return redis.error_reply('ERR accept_v3: occurred_ms must be empty or a positive integer')
end
local rid = ARGV[7]
if KEYS[1] ~= 'hr1:d:' .. ARGV[2]
    or KEYS[3] ~= 'hr1:m:' .. ARGV[1]
    or KEYS[4] ~= 'hr1:r:' .. rid .. ':q'
    or KEYS[5] ~= 'hr1:r:' .. rid .. ':s'
    or KEYS[10] ~= 'hr1:q:' .. rid
    or KEYS[12] ~= 'hr1:mi:' .. ARGV[1] then
  return redis.error_reply('ERR accept_v3: keys do not match the message and recipient identity')
end

local dedupKey, dedupAge, blobKey, queueKey, stateKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local readyKey, readySeqKey, leasesKey, blockedKey, markerKey, counterKey = KEYS[6], KEYS[7], KEYS[8], KEYS[9], KEYS[10], KEYS[11]
local metaKey = KEYS[12]

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, expected)
  local t = type_of(key)
  return t == 'none' or t == expected
end

-- 1. A blocked Recipient accepts nothing.
if redis.call('EXISTS', markerKey) == 1 then
  return {'recipient_blocked'}
end

-- 2. Deduplication: an existing record proves a duplicate.
if not has_type(dedupKey, 'hash') then
  return {'wrong_type'}
end
local function valid_ms(value)
  return value and string.match(value, '^[1-9]%d*$') and #value <= 19
    and (#value < 19 or value <= '9223372036854775807')
end
if redis.call('EXISTS', dedupKey) == 1 then
  local existing = redis.call('HMGET', dedupKey, 'message_id', 'body_digest', 'accepted_ms', 'expires_ms')
  if not existing[1] or existing[1] == '' or not existing[2] or existing[2] == ''
      or not valid_ms(existing[3]) or not valid_ms(existing[4])
      or not tonumber(existing[3]) or not tonumber(existing[4])
      or tonumber(existing[4]) <= tonumber(existing[3]) then
    return {'wrong_type'}
  end
  if existing[2] == ARGV[3] then
    return {'duplicate', existing[1]}
  end
  return {'duplicate_conflict', existing[1]}
end

-- 3. Every remaining key must have its expected type before it is read.
if not (has_type(dedupAge, 'zset') and has_type(blobKey, 'string') and has_type(queueKey, 'list')
    and has_type(stateKey, 'hash') and has_type(readyKey, 'zset') and has_type(readySeqKey, 'string')
    and has_type(leasesKey, 'zset') and has_type(blockedKey, 'zset') and has_type(counterKey, 'string')
    and has_type(metaKey, 'hash')) then
  return {'wrong_type'}
end
if type_of(blobKey) ~= 'none' or type_of(metaKey) ~= 'none' then
  return redis.error_reply('ERR accept_v3: message_id already stored')
end
-- INCR accepts only signed 64-bit integers and cannot increment MAXINT.
-- Check before the dedup record or queue can be written.
local seq = redis.call('GET', readySeqKey)
if seq and (not (seq == '0' or string.match(seq, '^[1-9]%d*$')) or #seq > 19
    or (#seq == 19 and seq >= '9223372036854775807')) then
  return {'wrong_type'}
end

-- 4. An empty queue must not have a head state: overwriting it could hide
-- a lease. The inconsistency is left for reconciliation to isolate.
local queueLen = redis.call('LLEN', queueKey)
if queueLen == 0 and redis.call('EXISTS', stateKey) == 1 then
  return {'state_inconsistent'}
end

-- 5. Capacity: nothing is created when any limit is reached.
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local retention = tonumber(ARGV[11])
local expires_ms = now_ms + retention
if queueLen + 1 > tonumber(ARGV[9]) then
  return {'recipient_capacity'}
end
if tonumber(redis.call('GET', counterKey) or '0') >= tonumber(ARGV[8]) then
  return {'global_capacity'}
end
-- At the cap, the oldest live record may be evicted early if it is valid
-- and old enough; it is validated completely before any write.
local evictDigest = nil
if redis.call('ZCOUNT', dedupAge, '(' .. (now_ms - retention), '+inf') >= tonumber(ARGV[10]) then
  local oldest = redis.call('ZRANGEBYSCORE', dedupAge, '(' .. (now_ms - retention), '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
  local digest, score = oldest[1], tonumber(oldest[2])
  if not digest or not score or score ~= math.floor(score) then
    return {'wrong_type'}
  end
  if score > now_ms - tonumber(ARGV[12]) then
    return {'dedup_capacity'}
  end
  local candidate = 'hr1:d:' .. digest
  if type_of(candidate) ~= 'hash' then
    return {'wrong_type'}
  end
  local accepted = redis.call('HGET', candidate, 'accepted_ms')
  if not valid_ms(accepted) or tonumber(accepted) ~= score then
    return {'wrong_type'}
  end
  evictDigest = digest
end

-- Writes.
local earlyEvicted = 0
if evictDigest then
  redis.call('DEL', 'hr1:d:' .. evictDigest)
  redis.call('ZREM', dedupAge, evictDigest)
  earlyEvicted = 1
end
redis.call('HSET', dedupKey,
  'message_id', ARGV[1],
  'body_digest', ARGV[3],
  'accepted_ms', now_ms,
  'expires_ms', expires_ms)
redis.call('PEXPIREAT', dedupKey, expires_ms)
redis.call('ZADD', dedupAge, now_ms, ARGV[2])
redis.call('SET', blobKey, ARGV[6])
redis.call('HSET', metaKey, 'dedup_identity_digest', ARGV[2])
redis.call('RPUSH', queueKey, ARGV[1])
if queueLen == 0 then
  -- The message becomes the queue head: fresh head state and a ready
  -- membership with a new fairness sequence.
  redis.call('HSET', stateKey,
    'status', 'ready',
    'head_message_id', ARGV[1],
    'delivery_cycle', 1,
    'attempt', 1)
  local nextSeq = redis.call('INCR', readySeqKey)
  redis.call('ZADD', readyKey, nextSeq, rid)
end
redis.call('INCR', counterKey)

-- Bounded pruning of index members whose records have expired by TTL.
local expired = redis.call('ZRANGEBYSCORE', dedupAge, '-inf', now_ms - retention, 'LIMIT', 0, 100)
if #expired > 0 then
  redis.call('ZREM', dedupAge, unpack(expired))
end

return {'accepted', now_ms, earlyEvicted}
