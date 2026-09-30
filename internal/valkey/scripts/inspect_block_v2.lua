-- inspect_block_v2: read one Recipient's block and state atomically without
-- any write. It applies exactly the checks, value parsers, and order of
-- clear_block_v2, but collects every violated invariant instead of stopping
-- at the first, so the first reported invariant is the one clear would
-- refuse with. It never returns a Delivery Token or payload.
--
-- KEYS[1] ready index    hr1:ready
-- KEYS[2] lease index    hr1:leases
-- KEYS[3] retry index    hr1:retries
-- KEYS[4] blocked index  hr1:blocked
--
-- ARGV[1] recipient_identity
-- ARGV[2] message check bound (per-recipient queue limit)
-- ARGV[3] key prefix ("hr1")
--
-- Invariants, in clear_block_v2 order: marker_type (marker not a Hash),
-- marker_fields (detected_ms not a positive integer or reason_code empty),
-- unsupported_key_type, queue_head_mismatch, head_state_missing,
-- head_message_missing, queued_delivery_state_missing,
-- queued_delivery_state_invalid. inspect_block_v1 plus the queued
-- pending-state invariants of reconcile_recipient_v3. Index members of the wrong index type read as
-- absent.
--
-- Returns:
--   {"inspected", marker ("none" | "present" | "wrong_type"), detected_ms,
--    reason_code, queue_length, head_message_id, status, delivery_cycle,
--    attempt, lease_expires_ms, retry_at_ms, head_message_present (0|1),
--    in_ready, in_leases, in_retries, in_blocked (0|1), {invariants}}
-- String fields are "" when absent.

if #KEYS ~= 4 or #ARGV ~= 3 then
  return redis.error_reply('ERR inspect_block_v2: expected 4 keys and 3 arguments')
end
if ARGV[1] == '' or ARGV[3] == '' or not string.match(ARGV[2], '^[1-9][0-9]*$') then
  return redis.error_reply('ERR inspect_block_v2: invalid arguments')
end
local rid, prefix = ARGV[1], ARGV[3]
if KEYS[1] ~= prefix .. ':ready' or KEYS[2] ~= prefix .. ':leases' or KEYS[3] ~= prefix .. ':retries'
    or KEYS[4] ~= prefix .. ':blocked' then
  return redis.error_reply('ERR inspect_block_v2: keys do not match the prefix')
end
local queueKey = prefix .. ':r:' .. rid .. ':q'
local stateKey = prefix .. ':r:' .. rid .. ':s'
local markerKey = prefix .. ':q:' .. rid

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function valid_ms(value)
  return value and string.match(value, '^[1-9]%d*$') and #value <= 15
end

-- Saved delivery state of a queued non-head message: nil when valid (a
-- complete pending pair, or neither pending field and no attempt history),
-- else the violated invariant. The attempt is never reset.
local function queued_state_issue(id)
  local metaKey = prefix .. ':mi:' .. id
  local mt = type_of(metaKey)
  local p = {false, false}
  if mt == 'hash' then
    p = redis.call('HMGET', metaKey, 'pending_delivery_cycle', 'pending_attempt')
  elseif mt ~= 'none' then
    return 'queued_delivery_state_invalid'
  end
  if p[1] or p[2] then
    if valid_ms(p[1]) and valid_ms(p[2]) then
      return nil
    end
    return 'queued_delivery_state_invalid'
  end
  if type_of(prefix .. ':a:' .. id) ~= 'none' then
    return 'queued_delivery_state_missing'
  end
  return nil
end
local function member(key)
  if type_of(key) ~= 'zset' then
    return 0
  end
  return redis.call('ZSCORE', key, rid) and 1 or 0
end

local invariants, seen = {}, {}
local function violate(inv)
  if not seen[inv] then
    seen[inv] = true
    invariants[#invariants + 1] = inv
  end
end

-- Marker.
local markerState, detected, reason = 'none', '', ''
local mt = type_of(markerKey)
if mt == 'hash' then
  markerState = 'present'
  local m = redis.call('HMGET', markerKey, 'detected_ms', 'reason_code')
  detected, reason = m[1] or '', m[2] or ''
  if not valid_ms(m[1]) or reason == '' then
    violate('marker_fields')
  end
elseif mt ~= 'none' then
  markerState = 'wrong_type'
  violate('marker_type')
end

local head = {id = '', status = '', cycle = '', attempt = '', lease = '', retry = ''}
local queueLen, headPresent = 0, 0
local qt, st = type_of(queueKey), type_of(stateKey)
if (qt ~= 'none' and qt ~= 'list') or (st ~= 'none' and st ~= 'hash') then
  violate('unsupported_key_type')
else
  local headID = nil
  if qt == 'list' then
    queueLen = redis.call('LLEN', queueKey)
    headID = redis.call('LINDEX', queueKey, 0)
  end
  local state = nil
  if st == 'hash' then
    state = redis.call('HMGET', stateKey, 'status', 'head_message_id', 'delivery_cycle', 'attempt',
      'lease_expires_ms', 'delivery_token', 'retry_at_ms')
    head = {id = state[2] or '', status = state[1] or '', cycle = state[3] or '', attempt = state[4] or '',
      lease = state[5] or '', retry = state[7] or ''}
  end
  -- The same checks, in the same order, as clear_block_v2.
  if not (qt == 'none' and st == 'none') then
    if qt == 'none' then
      violate('queue_head_mismatch')
    end
    if st == 'none' then
      violate('head_state_missing')
    end
    if state and qt == 'list' and state[2] ~= headID then
      violate('queue_head_mismatch')
    end
    if state and (not (state[3] and state[4])
        or (state[1] ~= 'ready' and state[1] ~= 'leased' and state[1] ~= 'retry_wait')) then
      violate('head_state_missing')
    end
    if qt == 'list' then
      local ids = redis.call('LRANGE', queueKey, 0, tonumber(ARGV[2]) - 1)
      for i, id in ipairs(ids) do
        local present = type_of(prefix .. ':m:' .. id) == 'string'
        if i == 1 and present then
          headPresent = 1
        end
        if not present then
          violate('head_message_missing')
        end
      end
      for i = 2, #ids do
        local issue = queued_state_issue(ids[i])
        if issue then
          violate(issue)
        end
      end
    end
    if state and state[1] == 'leased' and not (valid_ms(state[5]) and state[6]) then
      violate('head_state_missing')
    end
    if state and state[1] == 'retry_wait' and not valid_ms(state[7]) then
      violate('head_state_missing')
    end
  end
end

return {'inspected', markerState, detected, reason, queueLen, head.id, head.status, head.cycle, head.attempt,
  head.lease, head.retry, headPresent,
  member(KEYS[1]), member(KEYS[2]), member(KEYS[3]), member(KEYS[4]), invariants}
