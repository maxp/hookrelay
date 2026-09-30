-- delivery_state_v1: classify one message's delivery state atomically
-- without any write, so an operator can reconcile a replay whose response
-- was lost. It never returns a payload or Delivery Token. The Recipient
-- identity is only a locator (read by the caller from the message blob);
-- every classification re-reads the authoritative keys here.
--
-- ARGV[1] message_id
-- ARGV[2] recipient_identity ("" when the caller found no blob)
-- ARGV[3] key prefix ("hr1")
--
-- Classification, in order:
--   hr1:dl:<message_id> present           dead_lettered (dl.delivery_cycle)
--   queued at position 0 of the queue     leased | retry_wait | queued, from
--                                         the head state (queue_position head)
--   queued at a later position            queued (queue_position behind_head),
--                                         cycle = the saved pending pair, or
--                                         1 without attempt history
--   hr1:success:<message_id> present      acknowledged (success.delivery_cycle)
--   nothing                               not_found
-- A blob without a queue position, a head state that does not name the
-- message, an unknown status, malformed cycle fields, history without a
-- valid pending pair, or an unexpected key type is inconsistent + reason.
--
-- Returns:
--   {"found", state, delivery_cycle, queue_position ("" when not queued)}
--   {"not_found"}
--   {"inconsistent", reason}

if #KEYS ~= 0 or #ARGV ~= 3 then
  return redis.error_reply('ERR delivery_state_v1: expected 0 keys and 3 arguments')
end
if ARGV[1] == '' or ARGV[3] == '' then
  return redis.error_reply('ERR delivery_state_v1: invalid arguments')
end
local messageID, rid, prefix = ARGV[1], ARGV[2], ARGV[3]

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function valid_ms(value)
  return value and string.match(value, '^[1-9]%d*$') and #value <= 15
end

-- 1. Dead-lettered (the blob is retained with the record).
local dlKey = prefix .. ':dl:' .. messageID
local dt = type_of(dlKey)
if dt ~= 'none' then
  if dt ~= 'hash' then
    return {'inconsistent', 'unsupported_key_type'}
  end
  local cycle = redis.call('HGET', dlKey, 'delivery_cycle')
  if not valid_ms(cycle) then
    return {'inconsistent', 'dead_letter_record_invalid'}
  end
  return {'found', 'dead_lettered', tonumber(cycle), ''}
end

-- 2. Queued: the blob exists until acknowledgement.
local bt = type_of(prefix .. ':m:' .. messageID)
if bt ~= 'none' then
  if bt ~= 'string' or rid == '' then
    return {'inconsistent', 'message_invalid'}
  end
  local queueKey = prefix .. ':r:' .. rid .. ':q'
  local stateKey = prefix .. ':r:' .. rid .. ':s'
  if type_of(queueKey) ~= 'list' then
    return {'inconsistent', 'message_not_queued'}
  end
  local pos = redis.call('LPOS', queueKey, messageID)
  if not pos then
    return {'inconsistent', 'message_not_queued'}
  end
  if pos == 0 then
    if type_of(stateKey) ~= 'hash' then
      return {'inconsistent', 'head_state_missing'}
    end
    local st = redis.call('HMGET', stateKey, 'status', 'head_message_id', 'delivery_cycle')
    if st[2] ~= messageID then
      return {'inconsistent', 'queue_head_mismatch'}
    end
    if not valid_ms(st[3]) then
      return {'inconsistent', 'head_state_missing'}
    end
    local state = ({ready = 'queued', leased = 'leased', retry_wait = 'retry_wait'})[st[1]]
    if not state then
      return {'inconsistent', 'head_state_missing'}
    end
    return {'found', state, tonumber(st[3]), 'head'}
  end
  local metaKey = prefix .. ':mi:' .. messageID
  local mt = type_of(metaKey)
  local p = {false, false}
  if mt == 'hash' then
    p = redis.call('HMGET', metaKey, 'pending_delivery_cycle', 'pending_attempt')
  elseif mt ~= 'none' then
    return {'inconsistent', 'queued_delivery_state_invalid'}
  end
  if p[1] or p[2] then
    if not (valid_ms(p[1]) and valid_ms(p[2])) then
      return {'inconsistent', 'queued_delivery_state_invalid'}
    end
    return {'found', 'queued', tonumber(p[1]), 'behind_head'}
  end
  if type_of(prefix .. ':a:' .. messageID) ~= 'none' then
    return {'inconsistent', 'queued_delivery_state_missing'}
  end
  return {'found', 'queued', 1, 'behind_head'}
end

-- 3. Acknowledged, while the compact success metadata is retained.
local successKey = prefix .. ':success:' .. messageID
local sk = type_of(successKey)
if sk ~= 'none' then
  if sk ~= 'hash' then
    return {'inconsistent', 'unsupported_key_type'}
  end
  local cycle = redis.call('HGET', successKey, 'delivery_cycle')
  if not valid_ms(cycle) then
    return {'inconsistent', 'success_record_invalid'}
  end
  return {'found', 'acknowledged', tonumber(cycle), ''}
end

return {'not_found'}
