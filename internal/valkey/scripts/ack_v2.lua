-- ack_v2: acknowledge one Delivery Attempt by Delivery Token. ack_v1 plus
-- deleting the message metadata hr1:mi:<message_id> with the blob; a message
-- accepted before the metadata key existed acknowledges unchanged. The token
-- record (hr1:t:<digest>) locates the Recipient, message, and claim
-- operation; per-recipient keys are resolved inside the script (standalone
-- Valkey is a precondition). Every precondition is checked before the first
-- write; Valkey TIME is authoritative for acknowledged_ms and the lease
-- deadline comparison. The audit Stream is not written by acknowledgement.
--
-- KEYS[1] token record    hr1:t:<delivery_token_digest>
-- KEYS[2] ready index     hr1:ready
-- KEYS[3] ready sequence  hr1:ready_seq
-- KEYS[4] lease index     hr1:leases
-- KEYS[5] blocked index   hr1:blocked
-- KEYS[6] queued counter  hr1:stats:queued_messages
--
-- ARGV[1] delivery_token
-- ARGV[2] delivery_token_digest
-- ARGV[3] success_ttl_ms (24 hours)
-- ARGV[4] tombstone_ttl_ms (1 hour)
-- ARGV[5] key prefix ("hr1")
--
-- Preconditions in order: token record absent (not_found); terminal
-- acknowledged phase (already_acknowledged, idempotent repeat); block
-- marker present (recipient_blocked); expected key types (wrong_type); the
-- token is the current leased head's token, the head matches, and the lease
-- deadline has not passed (stale).
--
-- Returns:
--   {"acknowledged", message_id, acknowledged_ms, recipient_identity, delivery_cycle, attempt}
--   {"already_acknowledged", message_id, acknowledged_ms, "", 0, 0}
--   {"not_found"}
--   {"stale"}
--   {"recipient_blocked"}
--   {"wrong_type"}

if #KEYS ~= 6 or #ARGV ~= 5 then
  return redis.error_reply('ERR ack_v2: expected 6 keys and 5 arguments')
end
for i = 1, 5 do
  if ARGV[i] == '' then
    return redis.error_reply('ERR ack_v2: argument ' .. i .. ' is empty')
  end
end
for _, i in ipairs({3, 4}) do
  if not string.match(ARGV[i], '^[1-9][0-9]*$') then
    return redis.error_reply('ERR ack_v2: argument ' .. i .. ' must be a positive integer')
  end
end
local prefix = ARGV[5]
if KEYS[1] ~= prefix .. ':t:' .. ARGV[2] or KEYS[2] ~= prefix .. ':ready' or KEYS[3] ~= prefix .. ':ready_seq'
    or KEYS[4] ~= prefix .. ':leases' or KEYS[5] ~= prefix .. ':blocked' or KEYS[6] ~= prefix .. ':stats:queued_messages' then
  return redis.error_reply('ERR ack_v2: keys do not match the token and prefix')
end

local tokenKey, readyKey, readySeqKey, leasesKey, blockedKey, counterKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function has_type(key, expected)
  local t = type_of(key)
  return t == 'none' or t == expected
end

-- 1. Token record.
local tt = type_of(tokenKey)
if tt == 'none' then
  return {'not_found'}
end
if tt ~= 'hash' then
  return {'wrong_type'}
end
local tok = redis.call('HMGET', tokenKey, 'state', 'recipient_identity', 'message_id', 'operation_id', 'acknowledged_ms')

-- 2. Idempotent repeat of a completed acknowledgement.
if tok[1] == 'acknowledged' then
  return {'already_acknowledged', tok[3] or '', tonumber(tok[5]) or 0, '', 0, 0}
end
if tok[1] ~= 'active' or not tok[2] or not tok[3] or not tok[4] then
  return {'stale'}
end
local rid, messageID, operationID = tok[2], tok[3], tok[4]
local queueKey = prefix .. ':r:' .. rid .. ':q'
local stateKey = prefix .. ':r:' .. rid .. ':s'
local markerKey = prefix .. ':q:' .. rid
local blobKey = prefix .. ':m:' .. messageID
local metaKey = prefix .. ':mi:' .. messageID
local historyKey = prefix .. ':a:' .. messageID
local successKey = prefix .. ':success:' .. messageID
local opKey = prefix .. ':op:' .. operationID

-- 3. A blocked Recipient cannot be acknowledged.
if type_of(markerKey) ~= 'none' then
  return {'recipient_blocked'}
end

-- 4. Key types.
if not (has_type(queueKey, 'list') and has_type(stateKey, 'hash') and has_type(blobKey, 'string')
    and has_type(metaKey, 'hash')
    and has_type(historyKey, 'list') and has_type(successKey, 'hash') and has_type(opKey, 'hash')
    and has_type(readyKey, 'zset') and has_type(readySeqKey, 'string') and has_type(leasesKey, 'zset')
    and has_type(blockedKey, 'zset') and has_type(counterKey, 'string')) then
  return {'wrong_type'}
end

-- 5. The token must be the current, unexpired lease of the queue head.
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local state = redis.call('HMGET', stateKey, 'status', 'delivery_token', 'head_message_id', 'lease_expires_ms',
  'delivery_cycle', 'attempt', 'consumer_instance_id')
local head = redis.call('LINDEX', queueKey, 0)
if state[1] ~= 'leased' or state[2] ~= ARGV[1] or state[3] ~= messageID or head ~= messageID
    or (tonumber(state[4]) or 0) <= now_ms then
  return {'stale'}
end

-- Validate fallible numeric operations before deleting the head. A corrupt
-- counter or sequence cannot be allowed to leave an ack without its tombstone.
local queued = redis.call('GET', counterKey)
if not queued or not string.match(queued, '^[1-9]%d*$') or #queued > 19
    or (#queued == 19 and queued > '9223372036854775807') then
  return {'wrong_type'}
end
if redis.call('LINDEX', queueKey, 1) then
  local seq = redis.call('GET', readySeqKey)
  if seq and (not (seq == '0' or string.match(seq, '^[1-9]%d*$')) or #seq > 19
      or (#seq == 19 and seq >= '9223372036854775807')) then
    return {'wrong_type'}
  end
end

-- Writes. Compact success metadata carries only safe fields.
local recipientScope = string.match(rid, '^[^:]+:[^:]+:([a-z]+)')
local botPlatform = string.match(rid, '^([^:]+):')
local receivedMs = ''
local blob = redis.call('GET', blobKey)
if blob then
  local ok, decoded = pcall(cjson.decode, blob)
  if ok and type(decoded) == 'table' and decoded['received_ms'] then
    receivedMs = string.format('%d', decoded['received_ms'])
  end
end
redis.call('DEL', successKey)
redis.call('HSET', successKey,
  'recipient_scope', recipientScope or '',
  'bot_platform', botPlatform or '',
  'received_ms', receivedMs,
  'acknowledged_ms', now_ms,
  'delivery_cycle', state[5] or '1',
  'attempt_count', state[6] or '1')
if state[7] then
  redis.call('HSET', successKey, 'consumer_instance_id', state[7])
end
redis.call('PEXPIRE', successKey, tonumber(ARGV[3]))

redis.call('DEL', blobKey, metaKey, historyKey)
redis.call('LPOP', queueKey)
redis.call('ZREM', leasesKey, rid)
local nextHead = redis.call('LINDEX', queueKey, 0)
redis.call('DEL', stateKey)
if nextHead then
  redis.call('HSET', stateKey,
    'status', 'ready',
    'head_message_id', nextHead,
    'delivery_cycle', 1,
    'attempt', 1)
  local seq = redis.call('INCR', readySeqKey)
  redis.call('ZADD', readyKey, seq, rid)
else
  redis.call('DEL', queueKey)
  redis.call('ZREM', readyKey, rid)
end
redis.call('DECR', counterKey)

-- Terminal token phase: no plaintext token copies remain.
redis.call('DEL', tokenKey)
redis.call('HSET', tokenKey, 'state', 'acknowledged', 'message_id', messageID, 'acknowledged_ms', now_ms)
redis.call('PEXPIRE', tokenKey, tonumber(ARGV[4]))
local opTTL = redis.call('PTTL', opKey)
if opTTL > 0 then
  local argsDigest = redis.call('HGET', opKey, 'args_digest')
  redis.call('DEL', opKey)
  redis.call('HSET', opKey, 'kind', 'claim', 'args_digest', argsDigest or '', 'state', 'no_longer_active')
  redis.call('PEXPIRE', opKey, opTTL)
end

return {'acknowledged', messageID, now_ms, rid, tonumber(state[5]) or 1, tonumber(state[6]) or 1}
