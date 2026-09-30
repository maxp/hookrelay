-- reconcile_attempt_v1: validate the transient records that make one
-- non-due leased Recipient head an active Delivery Attempt. The leased head,
-- Delivery Token record, and claim operation are authoritative and are never
-- rewritten or deleted. Only the derived lease-index score may be repaired;
-- an isolatable mismatch creates the bounded active_attempt_inconsistent
-- Recipient block.
--
-- KEYS[1] ready index    hr1:ready
-- KEYS[2] lease index    hr1:leases
-- KEYS[3] retry index    hr1:retries
-- KEYS[4] blocked index  hr1:blocked
--
-- ARGV[1] mode: verify | block
-- ARGV[2] recipient_identity
-- ARGV[3] expected delivery_token_digest (64 lowercase hex for verify,
--         empty for block)
-- ARGV[4] detailed reason (empty for verify, token_digest_mismatch for block)
-- ARGV[5] claim operation TTL in milliseconds
-- ARGV[6] key prefix ("hr1")
--
-- Returns:
--   {"consistent"} | {"repaired"} | {"not_leased"} |
--   {"already_blocked"} | {"blocked", reason} | {"legacy"} |
--   {"changed"} | {"wrong_type", reason}

if #KEYS ~= 4 or #ARGV ~= 6 then
  return redis.error_reply('ERR reconcile_attempt_v1: expected 4 keys and 6 arguments')
end
local mode, rid, expectedDigest, detail, claimTTLArg, prefix = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]
local function valid_digest(value)
  return value and #value == 64 and string.match(value, '^[0-9a-f]+$')
end
local function valid_positive(value)
  return value and string.match(value, '^[1-9]%d*$') and #value <= 15
end
if rid == '' or prefix == '' or not valid_positive(claimTTLArg) then
  return redis.error_reply('ERR reconcile_attempt_v1: invalid arguments')
end
if mode == 'verify' then
  -- An empty expected digest is accepted only so the script can classify an
  -- accepted pre-v3 leased state as legacy. A current digest must be fenced
  -- with its exact 64-hex value.
  if (expectedDigest ~= '' and not valid_digest(expectedDigest)) or detail ~= '' then
    return redis.error_reply('ERR reconcile_attempt_v1: invalid verify arguments')
  end
elseif mode == 'block' then
  if expectedDigest ~= '' or detail ~= 'token_digest_mismatch' then
    return redis.error_reply('ERR reconcile_attempt_v1: invalid block arguments')
  end
else
  return redis.error_reply('ERR reconcile_attempt_v1: invalid mode')
end
if KEYS[1] ~= prefix .. ':ready' or KEYS[2] ~= prefix .. ':leases' or KEYS[3] ~= prefix .. ':retries'
    or KEYS[4] ~= prefix .. ':blocked' then
  return redis.error_reply('ERR reconcile_attempt_v1: keys do not match the prefix')
end
local readyKey, leasesKey, retriesKey, blockedKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local queueKey = prefix .. ':r:' .. rid .. ':q'
local stateKey = prefix .. ':r:' .. rid .. ':s'
local markerKey = prefix .. ':q:' .. rid
local claimTTL = tonumber(claimTTLArg)

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
for _, key in ipairs({readyKey, leasesKey, retriesKey, blockedKey}) do
  local t = type_of(key)
  if t ~= 'none' and t ~= 'zset' then
    return {'wrong_type', 'lease_index_type'}
  end
end

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local function align_blocked(detected)
  redis.call('ZREM', readyKey, rid)
  redis.call('ZREM', leasesKey, rid)
  redis.call('ZREM', retriesKey, rid)
  redis.call('ZADD', blockedKey, detected, rid)
end
local markerType = type_of(markerKey)
if markerType ~= 'none' then
  if markerType ~= 'hash' then
    return {'wrong_type', 'active_attempt_structure'}
  end
  local detected = redis.call('HGET', markerKey, 'detected_ms')
  if not valid_positive(detected) then
    detected = tostring(now_ms)
  end
  align_blocked(detected)
  return {'already_blocked'}
end
local function block(reason)
  redis.call('HSET', markerKey, 'detected_ms', now_ms, 'reason_code', 'active_attempt_inconsistent')
  align_blocked(now_ms)
  return {'blocked', reason}
end
if mode == 'block' then
  return block(detail)
end

local queueType, stateType = type_of(queueKey), type_of(stateKey)
if stateType ~= 'hash' then
  return {'not_leased'}
end
local state = redis.call('HMGET', stateKey, 'status', 'head_message_id', 'delivery_cycle', 'attempt',
  'claimed_ms', 'attempt_started_ms', 'lease_expires_ms', 'delivery_token', 'delivery_token_digest')
if state[1] ~= 'leased' then
  return {'not_leased'}
end
if queueType ~= 'list' or redis.call('LINDEX', queueKey, 0) ~= state[2] then
  return block('active_attempt_structure')
end
local started = state[6] or state[5]
if not (state[2] and state[2] ~= '' and valid_positive(state[3]) and valid_positive(state[4])
    and valid_positive(state[5]) and valid_positive(started) and valid_positive(state[7])
    and state[8] and state[8] ~= '') then
  return block('active_attempt_state')
end
-- Leases created before claim_v3 have no digest locator. Their accepted
-- recovery remains the existing recipient/TTL-only behavior.
if not state[9] then
  return {'legacy'}
end
if not valid_digest(state[9]) then
  return block('active_attempt_state')
end
if state[9] ~= expectedDigest then
  return {'changed'}
end

local tokenKey = prefix .. ':t:' .. state[9]
local tokenType = type_of(tokenKey)
if tokenType == 'none' then
  return block('token_missing')
end
if tokenType ~= 'hash' then
  return block('token_type')
end
local token = redis.call('HMGET', tokenKey, 'state', 'recipient_identity', 'message_id', 'operation_id', 'claimed_ms', 'lease_expires_ms')
if token[1] ~= 'active' or token[2] ~= rid or token[3] ~= state[2] or not token[4] or token[4] == ''
    or token[5] ~= state[5] or token[6] ~= state[7] then
  return block('token_mismatch')
end

local operationKey = prefix .. ':op:' .. token[4]
local operationType = type_of(operationKey)
if operationType == 'none' then
  return block('claim_operation_missing')
end
if operationType ~= 'hash' then
  return block('claim_operation_type')
end
local operation = redis.call('HMGET', operationKey, 'kind', 'state', 'delivery_token', 'message_id',
  'recipient_identity', 'delivery_cycle', 'attempt', 'claimed_ms', 'lease_expires_ms', 'args_digest')
if operation[1] ~= 'claim' or operation[2] ~= 'active' or operation[3] ~= state[8]
    or operation[4] ~= state[2] or operation[5] ~= rid or operation[6] ~= state[3]
    or operation[7] ~= state[4] or operation[8] ~= state[5]
    -- The idempotent claim response retains its original deadline after an
    -- extension; only head state and token record track the current deadline.
    or not valid_positive(operation[9]) or tonumber(operation[9]) <= tonumber(state[5])
    or tonumber(operation[9]) > tonumber(state[7])
    or not operation[10] or operation[10] == '' then
  return block('claim_operation_mismatch')
end

local tokenTTL = redis.call('PTTL', tokenKey)
local operationTTL = redis.call('PTTL', operationKey)
if operationTTL <= 0 or operationTTL > claimTTL then
  return block('claim_operation_ttl')
end
local remainingLease = tonumber(state[7]) - now_ms
if tokenTTL <= 0 or remainingLease <= 0 or tokenTTL < remainingLease or tokenTTL > remainingLease + claimTTL then
  return block('token_ttl')
end

if tonumber(redis.call('ZSCORE', leasesKey, rid)) ~= tonumber(state[7]) then
  redis.call('ZADD', leasesKey, state[7], rid)
  return {'repaired'}
end
return {'consistent'}
