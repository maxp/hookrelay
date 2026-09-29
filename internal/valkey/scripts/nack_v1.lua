-- nack_v1: negatively acknowledge one Delivery Attempt by Delivery Token.
-- The failed attempt is appended to the attempt history and the head waits
-- in retry_wait for the server-chosen delay; the message stays the queue
-- head. The token record (hr1:t:<digest>) locates the Recipient, message,
-- and claim operation; per-recipient keys are resolved inside the script
-- (standalone Valkey is a precondition). Every precondition is checked
-- before the first write; Valkey TIME is authoritative for completed_ms,
-- retry_at_ms, and the lease deadline comparison.
--
-- KEYS[1] token record    hr1:t:<delivery_token_digest>
-- KEYS[2] ready index     hr1:ready
-- KEYS[3] ready sequence  hr1:ready_seq
-- KEYS[4] lease index     hr1:leases
-- KEYS[5] retry index     hr1:retries
-- KEYS[6] blocked index   hr1:blocked
-- KEYS[7] DLQ index       hr1:dlq
-- KEYS[8] queued counter  hr1:stats:queued_messages
--
-- ARGV[1] delivery_token
-- ARGV[2] delivery_token_digest
-- ARGV[3] reason_code (empty, or 1-64 characters of [A-Za-z0-9_.:-])
-- ARGV[4] retry_delays_ms: comma-separated positive integers, entry n is
--         the drawn effective delay after failed attempt n; exactly
--         max_attempts - 1 entries (empty when max_attempts is 1)
-- ARGV[5] max_attempts
-- ARGV[6] tombstone_ttl_ms (1 hour)
-- ARGV[7] key prefix ("hr1")
--
-- Preconditions in order: token record absent (not_found); nacked phase
-- (already_nacked, idempotent repeat with the recorded result);
-- acknowledged phase (already_acknowledged); any other non-active phase,
-- including expired (stale); block marker present (recipient_blocked);
-- expected key types and well-formed state and history (wrong_type); the
-- token is the current leased head's token, the head matches, and the
-- lease deadline has not passed (stale); the failed attempt is below
-- max_attempts (attempts_exhausted: refused without mutation until the
-- dead-letter transition exists).
--
-- The history entry is compact JSON with fields in a fixed order:
--   {"kind":"attempt","delivery_cycle","attempt","claimed_ms",
--    "lease_expires_ms","completed_ms","outcome":"nack","reason_code"?,
--    "consumer_instance_id"?}
-- When more than 10 Delivery Cycles are present, the oldest cycles are
-- folded into one leading {"kind":"archived_cycles_summary",
-- "archived_cycles","archived_attempts","first_archived_ms",
-- "last_archived_ms"} entry (earliest archived claimed_ms, latest archived
-- completed_ms), merged with an existing summary.
--
-- Returns:
--   {"retry_scheduled", message_id, attempt, retry_at_ms, recipient_identity, delivery_cycle, claimed_ms, completed_ms}
--   {"already_nacked", result, message_id, attempt, retry_at_ms}
--   {"already_acknowledged"}
--   {"not_found"}
--   {"stale"}
--   {"recipient_blocked"}
--   {"attempts_exhausted"}
--   {"wrong_type"}

if #KEYS ~= 8 or #ARGV ~= 7 then
  return redis.error_reply('ERR nack_v1: expected 8 keys and 7 arguments')
end
for _, i in ipairs({1, 2, 5, 6, 7}) do
  if ARGV[i] == '' then
    return redis.error_reply('ERR nack_v1: argument ' .. i .. ' is empty')
  end
end
for _, i in ipairs({5, 6}) do
  if not string.match(ARGV[i], '^[1-9][0-9]*$') or #ARGV[i] > 15 then
    return redis.error_reply('ERR nack_v1: argument ' .. i .. ' must be a positive integer')
  end
end
local reasonCode = ARGV[3]
if reasonCode ~= '' and (#reasonCode > 64 or not string.match(reasonCode, '^[%w_.:%-]+$')) then
  return redis.error_reply('ERR nack_v1: reason_code is malformed')
end
local maxAttempts = tonumber(ARGV[5])
local delays = {}
for d in string.gmatch(ARGV[4] ~= '' and ARGV[4] .. ',' or '', '([^,]*),') do
  if not string.match(d, '^[1-9][0-9]*$') or #d > 15 then
    return redis.error_reply('ERR nack_v1: retry_delays_ms must be positive integers')
  end
  delays[#delays + 1] = tonumber(d)
end
if #delays ~= maxAttempts - 1 then
  return redis.error_reply('ERR nack_v1: retry_delays_ms must have max_attempts - 1 entries')
end
local prefix = ARGV[7]
if KEYS[1] ~= prefix .. ':t:' .. ARGV[2] or KEYS[2] ~= prefix .. ':ready' or KEYS[3] ~= prefix .. ':ready_seq'
    or KEYS[4] ~= prefix .. ':leases' or KEYS[5] ~= prefix .. ':retries' or KEYS[6] ~= prefix .. ':blocked'
    or KEYS[7] ~= prefix .. ':dlq' or KEYS[8] ~= prefix .. ':stats:queued_messages' then
  return redis.error_reply('ERR nack_v1: keys do not match the token and prefix')
end

local tokenKey, readyKey, readySeqKey, leasesKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local retriesKey, blockedKey, dlqKey, counterKey = KEYS[5], KEYS[6], KEYS[7], KEYS[8]

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

-- 1. Token record.
local tt = type_of(tokenKey)
if tt == 'none' then
  return {'not_found'}
end
if tt ~= 'hash' then
  return {'wrong_type'}
end
local tok = redis.call('HMGET', tokenKey, 'state', 'recipient_identity', 'message_id', 'operation_id',
  'result', 'attempt', 'retry_at_ms')

-- 2. Idempotent repeat of a completed negative acknowledgement.
if tok[1] == 'nacked' then
  return {'already_nacked', tok[5] or '', tok[3] or '', tonumber(tok[6]) or 0, tonumber(tok[7]) or 0}
end
if tok[1] == 'acknowledged' then
  return {'already_acknowledged'}
end
if tok[1] ~= 'active' or not tok[2] or not tok[3] or not tok[4] then
  return {'stale'}
end
local rid, messageID, operationID = tok[2], tok[3], tok[4]
local queueKey = prefix .. ':r:' .. rid .. ':q'
local stateKey = prefix .. ':r:' .. rid .. ':s'
local markerKey = prefix .. ':q:' .. rid
local historyKey = prefix .. ':a:' .. messageID
local opKey = prefix .. ':op:' .. operationID

-- 3. A blocked Recipient cannot be negatively acknowledged.
if type_of(markerKey) ~= 'none' then
  return {'recipient_blocked'}
end

-- 4. Key types.
if not (has_type(queueKey, 'list') and has_type(stateKey, 'hash') and has_type(historyKey, 'list')
    and has_type(opKey, 'hash') and has_type(readyKey, 'zset') and has_type(readySeqKey, 'string')
    and has_type(leasesKey, 'zset') and has_type(retriesKey, 'zset') and has_type(blockedKey, 'zset')
    and has_type(dlqKey, 'zset') and has_type(counterKey, 'string')) then
  return {'wrong_type'}
end

-- 5. The token must be the current, unexpired lease of the queue head.
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)
local state = redis.call('HMGET', stateKey, 'status', 'delivery_token', 'head_message_id', 'lease_expires_ms',
  'delivery_cycle', 'attempt', 'claimed_ms', 'consumer_instance_id')
local head = redis.call('LINDEX', queueKey, 0)
if state[1] ~= 'leased' or state[2] ~= ARGV[1] or state[3] ~= messageID or head ~= messageID
    or (tonumber(state[4]) or 0) <= now_ms then
  return {'stale'}
end
if not (valid_ms(state[4]) and valid_ms(state[5]) and valid_ms(state[6]) and valid_ms(state[7])) then
  return {'wrong_type'}
end
local cycle, attempt = tonumber(state[5]), tonumber(state[6])

-- 6. Parse the existing history before any write: a malformed entry is
-- corrupt state, never silently dropped by the archive fold.
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

-- 7. The last attempt needs the dead-letter transition.
if attempt >= maxAttempts then
  return {'attempts_exhausted'}
end

-- Writes.
local retryAt = now_ms + delays[attempt]
local entry = string.format(
  '{"kind":"attempt","delivery_cycle":%d,"attempt":%d,"claimed_ms":%d,"lease_expires_ms":%d,"completed_ms":%d,"outcome":"nack"',
  cycle, attempt, tonumber(state[7]), tonumber(state[4]), now_ms)
if reasonCode ~= '' then
  entry = entry .. ',"reason_code":"' .. reasonCode .. '"'
end
local instance = state[8]
if instance and #instance <= 64 and string.match(instance, '^[%w_.:%-]+$') then
  entry = entry .. ',"consumer_instance_id":"' .. instance .. '"'
end
entry = entry .. '}'
entries[#entries + 1] = {raw = entry, e = {kind = 'attempt', delivery_cycle = cycle, claimed_ms = tonumber(state[7]), completed_ms = now_ms}}

-- Fold the oldest cycles beyond the latest 10 into the leading summary.
local cycles, seen = {}, {}
for _, x in ipairs(entries) do
  local c = x.e['delivery_cycle']
  if x.e['kind'] == 'attempt' and not seen[c] then
    seen[c] = true
    cycles[#cycles + 1] = c
  end
end
if #cycles > 10 then
  table.sort(cycles)
  local archived = {}
  for i = 1, #cycles - 10 do
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
  sum.cycles = sum.cycles + (#cycles - 10)
  redis.call('DEL', historyKey)
  redis.call('RPUSH', historyKey, string.format(
    '{"kind":"archived_cycles_summary","archived_cycles":%d,"archived_attempts":%d,"first_archived_ms":%d,"last_archived_ms":%d}',
    sum.cycles, sum.attempts, sum.first, sum.last))
  redis.call('RPUSH', historyKey, unpack(kept))
else
  redis.call('RPUSH', historyKey, entry)
end

-- The head waits for its retry; no lease fields or plaintext token remain.
redis.call('DEL', stateKey)
redis.call('HSET', stateKey,
  'status', 'retry_wait',
  'head_message_id', messageID,
  'delivery_cycle', cycle,
  'attempt', attempt + 1,
  'retry_at_ms', retryAt)
redis.call('ZREM', leasesKey, rid)
redis.call('ZREM', readyKey, rid)
redis.call('ZADD', retriesKey, retryAt, rid)

-- Terminal token phase with the recorded result.
redis.call('DEL', tokenKey)
redis.call('HSET', tokenKey, 'state', 'nacked', 'result', 'retry_scheduled', 'message_id', messageID,
  'attempt', attempt, 'retry_at_ms', retryAt)
redis.call('PEXPIRE', tokenKey, tonumber(ARGV[6]))
local opTTL = redis.call('PTTL', opKey)
if opTTL > 0 then
  local argsDigest = redis.call('HGET', opKey, 'args_digest')
  redis.call('DEL', opKey)
  redis.call('HSET', opKey, 'kind', 'claim', 'args_digest', argsDigest or '', 'state', 'no_longer_active')
  redis.call('PEXPIRE', opKey, opTTL)
end

return {'retry_scheduled', messageID, attempt, retryAt, rid, cycle, tonumber(state[7]), now_ms}
