-- reconcile_message_v1: validate one Canonical Message lifecycle without
-- returning payload, token, or credential data. It may isolate a queue-local
-- ambiguity behind the bounded message_lifecycle_inconsistent Recipient
-- block, or delete only metadata/history proven orphaned by an atomic recheck.
--
-- KEYS[1..4] hr1:ready, hr1:leases, hr1:retries, hr1:blocked
-- ARGV[1] mode: inspect | delete_orphans
-- ARGV[2] message_id
-- ARGV[3] recipient_identity (empty when no queue locator is known)
-- ARGV[4] queue_position: head | behind_head | none
-- ARGV[5] dedup retention milliseconds
-- ARGV[6] key prefix (hr1)
--
-- Returns:
-- consistent|legacy|already_blocked + lifecycle
-- blocked|inconsistent + detailed reason
-- orphan_records + metadata_present, history_present
-- removed_orphans + removed_count
-- wrong_type + reason

if #KEYS ~= 4 or #ARGV ~= 6 then
  return redis.error_reply('ERR reconcile_message_v1: expected 4 keys and 6 arguments')
end
local mode, id, rid, position, retentionArg, prefix = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]
if (mode ~= 'inspect' and mode ~= 'delete_orphans') or id == '' or prefix == ''
    or (position ~= 'head' and position ~= 'behind_head' and position ~= 'none')
    or not string.match(retentionArg, '^[1-9]%d*$') or #retentionArg > 15 then
  return redis.error_reply('ERR reconcile_message_v1: invalid arguments')
end
if (position == 'none' and rid ~= '') or (position ~= 'none' and rid == '') then
  return redis.error_reply('ERR reconcile_message_v1: invalid locator')
end
if KEYS[1] ~= prefix .. ':ready' or KEYS[2] ~= prefix .. ':leases' or KEYS[3] ~= prefix .. ':retries'
    or KEYS[4] ~= prefix .. ':blocked' then
  return redis.error_reply('ERR reconcile_message_v1: keys do not match the prefix')
end
local readyKey, leasesKey, retriesKey, blockedKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local blobKey, metaKey, historyKey = prefix .. ':m:' .. id, prefix .. ':mi:' .. id, prefix .. ':a:' .. id
local dlKey, successKey = prefix .. ':dl:' .. id, prefix .. ':success:' .. id
local queueKey, markerKey = '', ''
if rid ~= '' then
  queueKey, markerKey = prefix .. ':r:' .. rid .. ':q', prefix .. ':q:' .. rid
end
local retention = tonumber(retentionArg)

local function type_of(key)
  return redis.call('TYPE', key)['ok']
end
local function valid_int(v)
  return v and string.match(v, '^[1-9]%d*$') and #v <= 19
    and (#v < 19 or v <= '9223372036854775807')
end
local function valid_number(v)
  return type(v) == 'number' and v > 0 and v == math.floor(v)
end
local function valid_digest(v)
  return v and #v == 64 and string.match(v, '^[0-9a-f]+$')
end
local function bounded(v, max)
  return v and v ~= '' and #v <= max and string.match(v, '^[%w_.:%-]+$')
end
local function accepted_marker_reason(v)
  return v == 'queue_head_mismatch' or v == 'head_message_missing' or v == 'head_state_missing'
    or v == 'unsupported_key_type' or v == 'queued_delivery_state_missing'
    or v == 'queued_delivery_state_invalid' or v == 'active_attempt_inconsistent'
    or v == 'message_lifecycle_inconsistent'
end

for _, key in ipairs({readyKey, leasesKey, retriesKey, blockedKey}) do
  local t = type_of(key)
  if t ~= 'none' and t ~= 'zset' then
    return {'wrong_type', 'lifecycle_ambiguous'}
  end
end
local bt, mt, ht, dt, st = type_of(blobKey), type_of(metaKey), type_of(historyKey), type_of(dlKey), type_of(successKey)
if rid ~= '' then
  local qt, markerType = type_of(queueKey), type_of(markerKey)
  if qt ~= 'none' and qt ~= 'list' then return {'wrong_type', 'lifecycle_ambiguous'} end
  if markerType ~= 'none' and markerType ~= 'hash' then return {'wrong_type', 'marker_invalid'} end
end

local function isolate(reason)
  if rid == '' then return {'inconsistent', reason} end
  local markerType = type_of(markerKey)
  if markerType == 'hash' then
    local marker = redis.call('HMGET', markerKey, 'detected_ms', 'reason_code')
    if not valid_int(marker[1]) or not accepted_marker_reason(marker[2]) then
      return {'inconsistent', 'marker_invalid'}
    end
    redis.call('ZADD', blockedKey, marker[1], rid)
    redis.call('ZREM', readyKey, rid)
    redis.call('ZREM', leasesKey, rid)
    redis.call('ZREM', retriesKey, rid)
    return {'already_blocked', 'queued'}
  end
  local tm = redis.call('TIME')
  local now = tm[1] * 1000 + math.floor(tm[2] / 1000)
  redis.call('HSET', markerKey, 'detected_ms', now, 'reason_code', 'message_lifecycle_inconsistent')
  redis.call('ZADD', blockedKey, now, rid)
  redis.call('ZREM', readyKey, rid)
  redis.call('ZREM', leasesKey, rid)
  redis.call('ZREM', retriesKey, rid)
  return {'blocked', reason}
end

local queued = position ~= 'none'
if queued then
  if type_of(queueKey) ~= 'list' then return isolate('lifecycle_ambiguous') end
  local actual = nil
  for i, queuedID in ipairs(redis.call('LRANGE', queueKey, 0, -1)) do
    if queuedID == id then actual = i - 1 break end
  end
  if actual == nil or (position == 'head' and actual ~= 0) or (position == 'behind_head' and actual == 0) then
    return isolate('lifecycle_ambiguous')
  end
end

-- Queue-local type corruption can be isolated only after the global indexes,
-- marker type, and queue locator have been checked. Unlocated corruption holds.
for _, record in ipairs({
  {bt, 'string', 'message_invalid'}, {mt, 'hash', 'metadata_invalid'},
  {ht, 'list', 'history_invalid'}, {dt, 'hash', 'dead_letter_invalid'},
  {st, 'hash', 'success_invalid'}
}) do
  if record[1] ~= 'none' and record[1] ~= record[2] then
    return queued and isolate(record[3]) or {'wrong_type', record[3]}
  end
end

-- cjson.null is a present JSON value; nil alone means the envelope member is
-- absent. Nested fields and the payload's shape do not change this check.
local decoded, serialized = nil, nil
if bt == 'string' then
  local raw = redis.call('GET', blobKey)
  local ok
  ok, decoded = pcall(cjson.decode, raw)
  local recipient = ok and type(decoded) == 'table' and decoded['recipient'] or nil
  local payloadPresent = ok and type(decoded) == 'table' and decoded['payload'] ~= nil
  if not ok or type(decoded) ~= 'table' or decoded['message_id'] ~= id
      or type(decoded['received_ms']) ~= 'number' or decoded['received_ms'] <= 0 or decoded['received_ms'] ~= math.floor(decoded['received_ms'])
      or type(recipient) ~= 'table' or not recipient['scope'] or not recipient['bot_platform'] or not recipient['bot_id']
      or type(decoded['platform_event_type']) ~= 'string' or decoded['platform_event_type'] == '' or not payloadPresent then
    return queued and isolate('message_invalid') or {'inconsistent', 'message_invalid'}
  end
  serialized = recipient['bot_platform'] .. ':' .. recipient['bot_id'] .. ':' .. recipient['scope']
  if recipient['scope'] == 'chat' then
    if not recipient['chat_id'] or recipient['user_id'] then return queued and isolate('message_invalid') or {'inconsistent', 'message_invalid'} end
    serialized = serialized .. ':' .. recipient['chat_id']
  elseif recipient['scope'] == 'user' then
    if not recipient['user_id'] or recipient['chat_id'] then return queued and isolate('message_invalid') or {'inconsistent', 'message_invalid'} end
    serialized = serialized .. ':' .. recipient['user_id']
  elseif recipient['scope'] ~= 'bot' and recipient['scope'] ~= 'relay' then
    return queued and isolate('message_invalid') or {'inconsistent', 'message_invalid'}
  elseif recipient['chat_id'] or recipient['user_id'] then
    return queued and isolate('message_invalid') or {'inconsistent', 'message_invalid'}
  end
  if rid ~= '' and serialized ~= rid then return isolate('message_invalid') end
end

-- Mutually exclusive live lifecycle evidence.
if st == 'hash' and (bt ~= 'none' or mt ~= 'none' or ht ~= 'none' or dt ~= 'none' or queued) then
  return queued and isolate('success_overlap') or {'inconsistent', 'success_overlap'}
end
if dt == 'hash' and queued then return isolate('dead_letter_overlap') end
if bt == 'string' and not queued and dt == 'none' and st == 'none' then
  -- A missing/wrong-typed queue can lose its head's discovery locator. Keep
  -- that incident local only when the valid blob, authoritative head state,
  -- and valid protective marker all agree on its owner. Other lone blobs
  -- (including unrelated blobs beside any block) remain readiness holds.
  local ownerMarker = prefix .. ':q:' .. serialized
  local ownerState = prefix .. ':r:' .. serialized .. ':s'
  local ownerQueue = prefix .. ':r:' .. serialized .. ':q'
  if type_of(ownerMarker) == 'hash' and type_of(ownerState) == 'hash' and type_of(ownerQueue) ~= 'list' then
    local marker = redis.call('HMGET', ownerMarker, 'detected_ms', 'reason_code')
    if valid_int(marker[1]) and accepted_marker_reason(marker[2])
        and redis.call('HGET', ownerState, 'head_message_id') == id then
      return {'already_blocked', 'queued'}
    end
  end
  return {'inconsistent', 'message_orphan'}
end
if (queued or dt == 'hash') and bt == 'none' then
  return queued and isolate('message_missing') or {'inconsistent', 'message_missing'}
end

-- Metadata allowlist, pending pair, and optional live dedup cross-check.
local legacy = false
if mt == 'hash' then
  local all = redis.call('HGETALL', metaKey)
  local allowed = {dedup_identity_digest = true, pending_delivery_cycle = true, pending_attempt = true}
  for i = 1, #all, 2 do
    if not allowed[all[i]] then return queued and isolate('metadata_invalid') or {'inconsistent', 'metadata_invalid'} end
  end
  local meta = redis.call('HMGET', metaKey, 'dedup_identity_digest', 'pending_delivery_cycle', 'pending_attempt')
  if meta[1] and not valid_digest(meta[1]) then
    return queued and isolate('metadata_invalid') or {'inconsistent', 'metadata_invalid'}
  end
  if (meta[2] and not meta[3]) or (meta[3] and not meta[2]) or (meta[2] and (not valid_int(meta[2]) or not valid_int(meta[3]))) then
    return queued and isolate('pending_state_invalid') or {'inconsistent', 'pending_state_invalid'}
  end
  if position == 'behind_head' and ht ~= 'none' and not meta[2] then return isolate('pending_state_missing') end
  if meta[1] then
    local dedupKey = prefix .. ':d:' .. meta[1]
    local dedupType = type_of(dedupKey)
    if dedupType ~= 'none' then
      if dedupType ~= 'hash' then return queued and isolate('dedup_record_invalid') or {'inconsistent', 'dedup_record_invalid'} end
      local rec = redis.call('HMGET', dedupKey, 'message_id', 'accepted_ms', 'expires_ms')
      if rec[1] == id then
        local ttl = redis.call('PTTL', dedupKey)
        if not valid_int(rec[2]) or not valid_int(rec[3]) or tonumber(rec[3]) <= tonumber(rec[2])
            or ttl <= 0 or ttl > retention then
          return queued and isolate('dedup_record_invalid') or {'inconsistent', 'dedup_record_invalid'}
        end
      end
    end
  end
elseif queued and ht == 'none' then
  legacy = true
elseif queued or dt == 'hash' then
  if ht ~= 'none' then return queued and isolate('metadata_invalid') or {'inconsistent', 'metadata_invalid'} end
  legacy = true
end

-- Attempt history validation and ordering.
if ht == 'list' then
  local lastCycle, lastAttempt, explicitCycles = 0, 0, {}
  local cycleCount = 0
  for i, raw in ipairs(redis.call('LRANGE', historyKey, 0, -1)) do
    local ok, entry = pcall(cjson.decode, raw)
    if not ok or type(entry) ~= 'table' then return queued and isolate('history_invalid') or {'inconsistent', 'history_invalid'} end
    if entry['kind'] == 'archived_cycles_summary' and i == 1 then
      if not valid_number(entry['archived_cycles']) or not valid_number(entry['archived_attempts'])
          or not valid_number(entry['first_archived_ms']) or not valid_number(entry['last_archived_ms'])
          or entry['last_archived_ms'] < entry['first_archived_ms'] then
        return queued and isolate('history_invalid') or {'inconsistent', 'history_invalid'}
      end
    elseif entry['kind'] == 'attempt' then
      local c, a = entry['delivery_cycle'], entry['attempt']
      if not valid_number(c) or not valid_number(a) or not valid_number(entry['claimed_ms'])
          or not valid_number(entry['lease_expires_ms']) or entry['lease_expires_ms'] < entry['claimed_ms']
          or not valid_number(entry['completed_ms']) or entry['completed_ms'] < entry['claimed_ms']
          or (entry['outcome'] ~= 'nack' and entry['outcome'] ~= 'expired')
          or (entry['reason_code'] and not bounded(entry['reason_code'], 64))
          or (entry['consumer_instance_id'] and not bounded(entry['consumer_instance_id'], 64))
          or c < lastCycle or (c == lastCycle and a < lastAttempt) then
        return queued and isolate('history_invalid') or {'inconsistent', 'history_invalid'}
      end
      if not explicitCycles[c] then explicitCycles[c], cycleCount = true, cycleCount + 1 end
      lastCycle, lastAttempt = c, a
    else
      return queued and isolate('history_invalid') or {'inconsistent', 'history_invalid'}
    end
  end
  if cycleCount > 10 then return queued and isolate('history_invalid') or {'inconsistent', 'history_invalid'} end
end

if dt == 'hash' then
  local dl = redis.call('HMGET', dlKey, 'bot_platform', 'bot_id', 'recipient_scope', 'chat_id', 'user_id',
    'recipient_identity', 'dead_lettered_ms', 'dead_letter_reason', 'delivery_cycle', 'dedup_identity_digest')
  local scopeOK = (dl[3] == 'chat' and dl[4] and not dl[5]) or (dl[3] == 'user' and dl[5] and not dl[4])
    or ((dl[3] == 'bot' or dl[3] == 'relay') and not dl[4] and not dl[5])
  local blobRecipient = decoded and decoded['recipient'] or nil
  if not dl[1] or not dl[2] or not scopeOK or not dl[6] or not valid_int(dl[7])
      or (dl[8] ~= 'nack_exhausted' and dl[8] ~= 'expiry_exhausted') or not valid_int(dl[9])
      or (dl[10] and dl[10] ~= '' and (#dl[10] > 128 or not string.match(dl[10], '^[%w_.:%-]+$')))
      or not blobRecipient or dl[1] ~= blobRecipient['bot_platform'] or dl[2] ~= blobRecipient['bot_id'] then
    return {'inconsistent', 'dead_letter_invalid'}
  end
  local blobRID = blobRecipient['bot_platform'] .. ':' .. blobRecipient['bot_id'] .. ':' .. blobRecipient['scope']
  if blobRecipient['scope'] == 'chat' then blobRID = blobRID .. ':' .. blobRecipient['chat_id'] end
  if blobRecipient['scope'] == 'user' then blobRID = blobRID .. ':' .. blobRecipient['user_id'] end
  if dl[6] ~= blobRID then return {'inconsistent', 'dead_letter_invalid'} end
end

if st == 'hash' then
  local all = redis.call('HGETALL', successKey)
  local allowed = {recipient_scope = true, bot_platform = true, received_ms = true, acknowledged_ms = true,
    delivery_cycle = true, attempt_count = true, consumer_instance_id = true}
  for i = 1, #all, 2 do if not allowed[all[i]] then return {'inconsistent', 'success_invalid'} end end
  local s = redis.call('HMGET', successKey, 'recipient_scope', 'bot_platform', 'received_ms', 'acknowledged_ms',
    'delivery_cycle', 'attempt_count', 'consumer_instance_id')
  local ttl = redis.call('PTTL', successKey)
  if (s[1] ~= 'chat' and s[1] ~= 'user' and s[1] ~= 'bot' and s[1] ~= 'relay') or not s[2] or s[2] == ''
      or not valid_int(s[3]) or not valid_int(s[4]) or tonumber(s[4]) < tonumber(s[3])
      or not valid_int(s[5]) or not valid_int(s[6]) or (s[7] and not bounded(s[7], 64))
      or ttl <= 0 or ttl > 86400000 then
    return {'inconsistent', 'success_invalid'}
  end
end

if rid ~= '' and type_of(markerKey) == 'hash' then
  local marker = redis.call('HMGET', markerKey, 'detected_ms', 'reason_code')
  if not valid_int(marker[1]) or not accepted_marker_reason(marker[2]) then return {'inconsistent', 'marker_invalid'} end
end

if bt == 'none' and dt == 'none' and st == 'none' and not queued and (mt == 'hash' or ht == 'list') then
  local mp, hp = mt == 'hash' and 1 or 0, ht == 'list' and 1 or 0
  if mode == 'delete_orphans' then
    local removed = redis.call('DEL', metaKey, historyKey)
    return {'removed_orphans', removed}
  end
  return {'orphan_records', mp, hp}
end

local lifecycle = queued and 'queued' or (dt == 'hash' and 'dead_lettered' or (st == 'hash' and 'acknowledged' or 'none'))
if legacy then return {'legacy', lifecycle} end
if rid ~= '' and type_of(markerKey) == 'hash' then return {'already_blocked', lifecycle} end
return {'consistent', lifecycle}
