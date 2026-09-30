#!/usr/bin/env bash
# Compose smoke test: the Milestone 1 vertical slice, the Milestone 2
# failure path, and the Milestone 3 and 4 additions end to end against the
# pinned Compose stack, in production mode (AOF + noeviction gate).
#
#   start stack → create Telegram endpoint (CLI) → send signed fixture →
#   repeat and prove exactly one stored message → claim (wait_ms=0) → ack →
#   repeat ack and receive the recorded result → prove the queue empty →
#   check metrics and health → failure path: nack three times with observed
#   retries → fourth nack dead-letters → DLQ via CLI → replay via CLI →
#   delivery state via CLI → claim delivery_cycle=2 → ack → leave a lease
#   claimed → restart keeping the Valkey volume after the lease expired →
#   verify readiness (the expiry ran in startup reconciliation), the
#   persisted endpoint, and continued deduplication → Milestone 3: a waiting
#   claim woken by the ready-work notifier when a webhook arrives →
#   credential replacement through the CLI (create a second endpoint, list
#   the bot's endpoints, disable and delete the old one, the old path 404s,
#   the new one accepts) → Milestone 4: two dead letters → operations
#   summary → browser login (production cookie attributes) → cookie replay
#   refused without and accepted with the CSRF token → audited payload view
#   → deletion refused without and accepted with If-Match → audit list shows
#   the session actions → logout → the /ui/ panel with its CSP → metrics.
#
# The run uses its own Compose project, generated secrets, and free loopback
# ports, so it never touches a developer's .secrets/ or running stack.
# Usage: scripts/smoke.sh            (builds the image first)
#        SMOKE_KEEP=1 scripts/smoke.sh (leave the stack up on failure)
#        SMOKE_PUBLIC_PORT=… SMOKE_ADMIN_PORT=… scripts/smoke.sh (chosen ports)
set -euo pipefail

cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME="hookrelay-smoke-$$"
work="$(mktemp -d)"
mkdir -m 0700 "$work/secrets"

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }
public_port="${SMOKE_PUBLIC_PORT:-$(free_port)}"
admin_port="${SMOKE_ADMIN_PORT:-$(free_port)}"

step() { printf '\n==> %s\n' "$*"; }
fail() {
  printf 'SMOKE FAILED: %s\n' "$*" >&2
  compose logs --no-color --tail 80 hookrelay >&2 || true
  exit 1
}
expect() { # expect <description> <actual> <expected>
  [[ "$2" == "$3" ]] || fail "$1: got '$2', want '$3'"
  printf '    ok  %s = %s\n' "$1" "$2"
}

cat >"$work/override.yml" <<EOF
services:
  hookrelay:
    user: "$(id -u):$(id -g)"
    environment:
      HOOKRELAY_ENVIRONMENT: production
      HOOKRELAY_ADMIN_ORIGIN: https://admin.smoke.invalid
      HOOKRELAY_WEBHOOK_GLOBAL_RATE: "100"
      HOOKRELAY_WEBHOOK_GLOBAL_BURST: "100"
      HOOKRELAY_WEBHOOK_ENDPOINT_RATE: "10"
      HOOKRELAY_WEBHOOK_ENDPOINT_BURST: "10"
      HOOKRELAY_EXPECTED_PEAK_RATE: "1"
      # Short failure-path timing: retries become due quickly and a lease
      # left claimed expires while the process is down.
      HOOKRELAY_RETRY_DELAYS: "300ms,300ms,300ms"
      HOOKRELAY_INITIAL_LEASE_DURATION: "5s"
    ports: !override
      - "127.0.0.1:${public_port}:8080"
      - "127.0.0.1:${admin_port}:8081"
secrets:
  consumer:
    file: $work/secrets/consumer
  admin:
    file: $work/secrets/admin
EOF

compose() { docker compose -f docker-compose.yml -f "$work/override.yml" "$@"; }
vk() { compose exec -T valkey valkey-cli --raw "$@"; }
cleanup() {
  if [[ "${SMOKE_KEEP:-}" == "1" ]]; then
    echo "stack kept: COMPOSE_PROJECT_NAME=$COMPOSE_PROJECT_NAME, override $work/override.yml"
    return
  fi
  compose down -v --remove-orphans --rmi local >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

public="http://127.0.0.1:${public_port}"
admin="http://127.0.0.1:${admin_port}"
curl_() { curl --noproxy '*' -sS "$@"; }

wait_ready() {
  for _ in $(seq 1 60); do
    if [[ "$(curl_ -o /dev/null -w '%{http_code}' "$admin/health/ready" || true)" == "200" ]]; then
      return 0
    fi
    sleep 1
  done
  fail "hookrelay did not become ready"
}

step "build and start the stack"
compose build --quiet hookrelay
# The generator container mounts the declared secrets, so they must exist.
touch "$work/secrets/consumer" "$work/secrets/admin"
compose run --rm --no-deps -T --entrypoint /hookrelay hookrelay generate consumer-secret >"$work/secrets/consumer"
compose run --rm --no-deps -T --entrypoint /hookrelay hookrelay generate admin-secret >"$work/secrets/admin"
chmod 0600 "$work/secrets/"*
consumer_secret="$(tr -d '\n' <"$work/secrets/consumer")"
compose up -d --wait
wait_ready
expect "production readiness" "$(curl_ "$admin/health/ready")" '{"accepting_webhooks":true,"status":"ready"}'

step "create a Telegram endpoint through the Admin CLI"
credential="smoke-telegram-secret-01"
compose exec -T -e HOOKRELAY_WEBHOOK_CREDENTIAL="$credential" hookrelay \
  /hookrelay admin webhook create --type telegram --bot-id 424242 --identifier wh_smoke --output json >"$work/created.json"
expect "created endpoint" "$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["webhook_identifier"], d["bot_platform"], d["credential"]["configured"])' "$work/created.json")" "wh_smoke telegram True"

step "send the signed fixture twice"
fixture='{"update_id":777,"message":{"message_id":1,"date":1700000000,"chat":{"id":-100777,"type":"group"},"text":"smoke"}}'
send_webhook() {
  curl_ -o /dev/null -w '%{http_code}' -X POST "$public/webhook/telegram/wh_smoke" \
    -H 'Content-Type: application/json' -H "X-Telegram-Bot-Api-Secret-Token: $credential" -d "$fixture"
}
expect "first webhook" "$(send_webhook)" 200
expect "repeated webhook" "$(send_webhook)" 200
recipient='telegram:424242:chat:-100777'
expect "stored messages" "$(vk --scan --pattern 'hr1:m:*' | wc -l | tr -d ' ')" 1
expect "queue length" "$(vk LLEN "hr1:r:${recipient}:q")" 1

step "claim with wait_ms=0"
claim() { # claim <operation_id> [wait_ms]
  curl_ -X POST "$public/v1/deliveries/claim" -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $consumer_secret" -d "{\"operation_id\":\"$1\",\"wait_ms\":${2:-0}}" -w '\n%{http_code}'
}
json() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2], {"d": d}))' "$1" "$2"; }
out="$(claim 0195c4d8-0000-7000-8000-00000000a001)"
expect "claim status" "$(tail -n1 <<<"$out")" 200
body="$(head -n -1 <<<"$out")"
token="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["delivery"]["delivery_token"])' "$body")"
message_id="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["message"]["message_id"])' "$body")"
expect "claimed source event" "$(python3 -c 'import json,sys; m=json.loads(sys.argv[1])["message"]; print(m["source_event_id"], m["recipient"]["chat_id"])' "$body")" "777 -100777"
token_digest="$(printf '%s' "$token" | sha256sum | cut -d' ' -f1)"
expect "tombstone phase after claim" "$(vk HGET "hr1:t:${token_digest}" state)" active

step "acknowledge, then repeat the acknowledgement"
ack() {
  curl_ -X POST "$public/v1/deliveries/ack" -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $consumer_secret" -d "{\"delivery_token\":\"$token\"}" -w '\n%{http_code}'
}
first="$(ack)"
expect "ack status" "$(tail -n1 <<<"$first")" 200
second="$(ack)"
expect "repeated ack status" "$(tail -n1 <<<"$second")" 200
expect "repeated ack body" "$(head -n -1 <<<"$second")" "$(head -n -1 <<<"$first")"
expect "tombstone phase after ack" "$(vk HGET "hr1:t:${token_digest}" state)" acknowledged
expect "success metadata" "$(vk HGET "hr1:success:${message_id}" recipient_scope) $(vk HGET "hr1:success:${message_id}" attempt_count)" "chat 1"

step "prove the queue empty"
expect "queue keys" "$(vk --scan --pattern 'hr1:r:*' | wc -l | tr -d ' ')" 0
expect "stored messages" "$(vk --scan --pattern 'hr1:m:*' | wc -l | tr -d ' ')" 0
expect "queued counter" "$(vk GET hr1:stats:queued_messages)" 0
expect "empty claim" "$(tail -n1 <<<"$(claim 0195c4d8-0000-7000-8000-00000000a002)")" 204

step "check metrics and health"
metrics="$(curl_ "$admin/metrics")"
metric() { grep -E "^$1 " <<<"$metrics" | awk '{print $2}'; }
expect "accepted" "$(metric 'hookrelay_messages_accepted_total\{bot_platform="telegram",recipient_scope="chat"\}')" 1
expect "duplicates" "$(metric 'hookrelay_messages_duplicate_total\{webhook_type="telegram"\}')" 1
expect "acknowledged attempts" "$(metric 'hookrelay_delivery_attempts_total\{outcome="acknowledged",recipient_scope="chat"\}')" 1
expect "valkey connected" "$(metric 'hookrelay_valkey_connected')" 1
expect "valkey accept operations" "$(metric 'hookrelay_valkey_operations_total\{operation="accept",outcome="success"\}')" 2
expect "live" "$(curl_ -o /dev/null -w '%{http_code}' "$admin/health/live")" 200
expect "accepting webhooks" "$(curl_ -o /dev/null -w '%{http_code}' "$admin/health/accepting-webhooks")" 200

step "failure path: three nacks with observed retries"
fixture2='{"update_id":778,"message":{"message_id":2,"date":1700000000,"chat":{"id":-100888,"type":"group"},"text":"fail"}}'
send_fixture() {
  curl_ -o /dev/null -w '%{http_code}' -X POST "$public/webhook/telegram/wh_smoke" \
    -H 'Content-Type: application/json' -H "X-Telegram-Bot-Api-Secret-Token: $credential" -d "$1"
}
expect "failing webhook" "$(send_fixture "$fixture2")" 200
recipient2='telegram:424242:chat:-100888'
nack() {
  curl_ -X POST "$public/v1/deliveries/nack" -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $consumer_secret" -d "{\"delivery_token\":\"$1\",\"reason_code\":\"smoke_failure\"}" -w '\n%{http_code}'
}
for attempt in 1 2 3 4; do
  # A waiting claim picks the retry up once it is due (inline maintenance).
  out="$(claim "0195c4d8-0000-7000-8000-00000000b00${attempt}" 5000)"
  expect "claim attempt $attempt status" "$(tail -n1 <<<"$out")" 200
  body="$(head -n -1 <<<"$out")"
  expect "claimed cycle/attempt" "$(json "$body" 'd["delivery"]["delivery_cycle"], d["delivery"]["attempt"]')" "(1, $attempt)"
  failed_id="$(json "$body" 'd["message"]["message_id"]')"
  out="$(nack "$(json "$body" 'd["delivery"]["delivery_token"]')")"
  expect "nack $attempt status" "$(tail -n1 <<<"$out")" 200
  if [[ $attempt -lt 4 ]]; then
    expect "nack $attempt result" "$(json "$(head -n -1 <<<"$out")" 'd["status"], d["attempt"]')" "('retry_scheduled', $attempt)"
    expect "head state after nack $attempt" "$(vk HGET "hr1:r:${recipient2}:s" status)" retry_wait
  else
    expect "fourth nack result" "$(json "$(head -n -1 <<<"$out")" 'd["status"], d["delivery_cycle"]')" "('dead_lettered', 1)"
  fi
done
expect "attempt history" "$(vk LLEN "hr1:a:${failed_id}")" 4
expect "queue drained" "$(vk EXISTS "hr1:r:${recipient2}:q")" 0

step "dead letter through the Admin CLI"
admin_cli() { compose exec -T hookrelay /hookrelay admin "$@" --output json; }
dlq="$(admin_cli dlq list)"
expect "DLQ entry" "$(json "$dlq" '[(i["message_id"], i["delivery_cycle"], i["dead_letter_reason"], i["recipient"]["chat_id"]) for i in d["items"]]')" \
  "[('${failed_id}', 1, 'nack_exhausted', '-100888')]"
got="$(admin_cli dlq get --message-id "$failed_id")"
expect "DLQ history outcomes" "$(json "$got" '[a["outcome"] for a in d["attempts"]]')" "['nack', 'nack', 'nack', 'nack']"
replayed="$(admin_cli dlq replay --message-id "$failed_id" --yes)"
expect "replay" "$(json "$replayed" 'd["outcome"], d["delivery_cycle"], d["queue_position"], d["previous_delivery_cycle"]')" "('replayed', 2, 'head', 1)"
state="$(admin_cli message delivery-state --message-id "$failed_id")"
expect "delivery state" "$(json "$state" 'd["state"], d["delivery_cycle"], d["queue_position"]')" "('queued', 2, 'head')"
expect "DLQ after replay" "$(json "$(admin_cli dlq list)" 'len(d["items"])')" 0

step "claim the replayed message in delivery cycle 2 and acknowledge it"
out="$(claim 0195c4d8-0000-7000-8000-00000000b005)"
expect "replayed claim status" "$(tail -n1 <<<"$out")" 200
body="$(head -n -1 <<<"$out")"
expect "replayed claim" "$(json "$body" 'd["message"]["message_id"], d["delivery"]["delivery_cycle"], d["delivery"]["attempt"]')" "('${failed_id}', 2, 1)"
token="$(json "$body" 'd["delivery"]["delivery_token"]')"
expect "replayed ack" "$(tail -n1 <<<"$(ack)")" 200
expect "acknowledged state" "$(json "$(admin_cli message delivery-state --message-id "$failed_id")" 'd["state"], d["delivery_cycle"]')" "('acknowledged', 2)"

step "check failure-path metrics"
metrics="$(curl_ "$admin/metrics")"
expect "nack attempts" "$(metric 'hookrelay_delivery_attempts_total\{outcome="nack",recipient_scope="chat"\}')" 3
expect "dead-lettered attempts" "$(metric 'hookrelay_delivery_attempts_total\{outcome="dead_lettered",recipient_scope="chat"\}')" 1
expect "dead letters" "$(metric 'hookrelay_dead_letters_total\{reason="nack_exhausted",recipient_scope="chat"\}')" 1
expect "replays" "$(metric 'hookrelay_dead_letter_replays_total\{outcome="replayed"\}')" 1
expect "replay audit" "$(metric 'hookrelay_audit_events_total\{operation="dead_letter_replayed",outcome="success"\}')" 1

step "leave a lease claimed across a restart"
fixture3='{"update_id":779,"message":{"message_id":3,"date":1700000000,"chat":{"id":-100999,"type":"group"},"text":"stall"}}'
expect "stalled webhook" "$(send_fixture "$fixture3")" 200
recipient3='telegram:424242:chat:-100999'
out="$(claim 0195c4d8-0000-7000-8000-00000000b006)"
expect "stalled claim status" "$(tail -n1 <<<"$out")" 200
stalled_id="$(json "$(head -n -1 <<<"$out")" 'd["message"]["message_id"]')"
claimed_at="$(date +%s)"

step "restart the stack keeping the Valkey volume"
compose down
# The 5 s lease must pass while the process is down.
sleep $(( 7 - ($(date +%s) - claimed_at) > 0 ? 7 - ($(date +%s) - claimed_at) : 0 ))
compose up -d --wait
wait_ready
expect "due lease processed before readiness" \
  "$(compose logs --no-color hookrelay | grep '"event":"reconciliation_completed"' | tail -n1 | grep -o '"due_leases_processed":[0-9]*')" \
  '"due_leases_processed":1'
# Background maintenance may already have activated the retry (300 ms).
expect "expired lease became a retry" "$(vk HGET "hr1:r:${recipient3}:s" attempt) $(vk HGET "hr1:r:${recipient3}:s" delivery_token)" "2 "
expect "expired attempt recorded" "$(vk LINDEX "hr1:a:${stalled_id}" 0 | python3 -c 'import json,sys; print(json.load(sys.stdin)["outcome"])')" expired
compose exec -T hookrelay /hookrelay admin webhook get --type telegram --identifier wh_smoke --output json >"$work/got.json"
expect "persisted endpoint" "$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["bot_id"], d["enabled"])' "$work/got.json")" "424242 True"
expect "webhook after restart" "$(send_webhook)" 200
# Only the stalled message remains stored; the repeat created nothing.
expect "stored messages after repeat" "$(vk --scan --pattern 'hr1:m:*' | wc -l | tr -d ' ')" 1
metrics="$(curl_ "$admin/metrics")"
expect "deduplicated after restart" "$(metric 'hookrelay_messages_duplicate_total\{webhook_type="telegram"\}')" 1
expect "consistency issues" "$(grep -c '^hookrelay_consistency_issues_total' <<<"$metrics" || true)" 0

step "Milestone 3: a waiting claim wakes on a new webhook"
# Drain the stalled message's retry so the waiting claim below can only
# receive the new webhook.
out="$(claim 0195c4d8-0000-7000-8000-00000000c001 5000)"
expect "stalled retry claim" "$(json "$(head -n -1 <<<"$out")" 'd["message"]["message_id"], d["delivery"]["attempt"]')" "('${stalled_id}', 2)"
token="$(json "$(head -n -1 <<<"$out")" 'd["delivery"]["delivery_token"]')"
expect "stalled retry ack" "$(tail -n1 <<<"$(ack)")" 200
claim 0195c4d8-0000-7000-8000-00000000c002 10000 >"$work/waiting.out" &
waiter=$!
sleep 1
fixture4='{"update_id":780,"message":{"message_id":4,"date":1700000000,"chat":{"id":-100555,"type":"group"},"text":"wake"}}'
expect "wake-up webhook" "$(send_fixture "$fixture4")" 200
wait "$waiter"
out="$(cat "$work/waiting.out")"
expect "woken claim status" "$(tail -n1 <<<"$out")" 200
expect "woken claim message" "$(json "$(head -n -1 <<<"$out")" 'd["message"]["recipient"]["chat_id"]')" -100555
token="$(json "$(head -n -1 <<<"$out")" 'd["delivery"]["delivery_token"]')"
expect "woken claim ack" "$(tail -n1 <<<"$(ack)")" 200
metrics="$(curl_ "$admin/metrics")"
# The acceptance signal found the registered waiting claim.
expect "accept signal delivered" "$(python3 -c 'import sys; print(float(sys.argv[1] or 0) >= 1)' "$(metric 'hookrelay_ready_signals_total\{result="delivered",source="accept"\}')")" True

step "Milestone 3: credential replacement through the Admin CLI"
credential2="smoke-telegram-secret-02"
compose exec -T -e HOOKRELAY_WEBHOOK_CREDENTIAL="$credential2" hookrelay \
  /hookrelay admin webhook create --type telegram --bot-id 424242 --identifier wh_smoke2 --output json >/dev/null
expect "bot endpoints" "$(json "$(admin_cli bot webhooks --platform telegram --bot-id 424242)" 'sorted(i["webhook_identifier"] for i in d["items"])')" \
  "['wh_smoke', 'wh_smoke2']"
expect "endpoint list" "$(json "$(admin_cli webhook list --limit 1)" 'len(d["items"]), "next_cursor" in d')" "(1, True)"
disabled="$(admin_cli webhook disable --type telegram --identifier wh_smoke --yes)"
expect "disabled endpoint" "$(json "$disabled" 'd["enabled"], d["config_version"]')" "(False, 2)"
expect "webhook to the disabled endpoint" "$(send_webhook)" 404
expect "delete" "$(json "$(admin_cli webhook delete --type telegram --identifier wh_smoke --yes)" 'd["outcome"]')" deleted
expect "bot endpoints after delete" "$(json "$(admin_cli bot webhooks --platform telegram --bot-id 424242)" '[i["webhook_identifier"] for i in d["items"]]')" "['wh_smoke2']"
expect "webhook to the deleted endpoint" "$(send_webhook)" 404
expect "webhook to the new endpoint" "$(curl_ -o /dev/null -w '%{http_code}' -X POST "$public/webhook/telegram/wh_smoke2" \
  -H 'Content-Type: application/json' -H "X-Telegram-Bot-Api-Secret-Token: $credential2" \
  -d '{"update_id":781,"message":{"message_id":5,"date":1700000000,"chat":{"id":-100556,"type":"group"},"text":"new"}}')" 200
metrics="$(curl_ "$admin/metrics")"
expect "disable audit" "$(metric 'hookrelay_audit_events_total\{operation="webhook_endpoint_disabled",outcome="success"\}')" 1
expect "delete audit" "$(metric 'hookrelay_audit_events_total\{operation="webhook_endpoint_deleted",outcome="success"\}')" 1

step "Milestone 4: two dead letters for the operational views"
send2() { # send2 <update_id> <chat_id>
  curl_ -o /dev/null -w '%{http_code}' -X POST "$public/webhook/telegram/wh_smoke2" \
    -H 'Content-Type: application/json' -H "X-Telegram-Bot-Api-Secret-Token: $credential2" \
    -d "{\"update_id\":$1,\"message\":{\"message_id\":$1,\"date\":1700000000,\"chat\":{\"id\":$2,\"type\":\"group\"},\"text\":\"<img src=x onerror=alert(1)>\"}}"
}
expect "failing webhook A" "$(send2 790 -100557)" 200
expect "failing webhook B" "$(send2 791 -100558)" 200
dead=0
for n in $(seq 1 40); do
  out="$(claim "0195c4d8-0000-7000-8000-0000000d$(printf '%04d' "$n")" 5000)"
  [[ "$(tail -n1 <<<"$out")" == 200 ]] || continue
  body="$(head -n -1 <<<"$out")"
  token="$(json "$body" 'd["delivery"]["delivery_token"]')"
  if [[ "$(json "$body" 'd["message"]["recipient"]["chat_id"]')" == -100556 ]]; then
    ack >/dev/null
    continue
  fi
  [[ "$(json "$(head -n -1 <<<"$(nack "$token")")" 'd["status"]')" == dead_lettered ]] && dead=$((dead + 1))
  [[ $dead -eq 2 ]] && break
done
expect "dead-lettered messages" "$dead" 2
dlq="$(admin_cli dlq list)"
dl_replay="$(json "$dlq" '[i["message_id"] for i in d["items"] if i["recipient"]["chat_id"] == "-100557"][0]')"
dl_delete="$(json "$dlq" '[i["message_id"] for i in d["items"] if i["recipient"]["chat_id"] == "-100558"][0]')"
expect "summary dead letters" "$(json "$(admin_cli operations summary)" 'd["dead_letters"]["count"], d["readiness"]["ready"]')" "(2, True)"

step "Milestone 4: browser session with CSRF"
origin='https://admin.smoke.invalid'
admin_secret="$(tr -d '\n' <"$work/secrets/admin")"
curl_ -D "$work/login.headers" -o /dev/null -X POST "$admin/admin/v1/session" -H "Origin: $origin" \
  -H 'Content-Type: application/json' -d "{\"admin_secret\":\"$admin_secret\"}"
expect "login status" "$(head -n1 "$work/login.headers" | awk '{print $2}')" 204
set_cookie="$(grep -i '^set-cookie: hookrelay_admin=' "$work/login.headers" | tr -d '\r')"
for attr in HttpOnly Secure SameSite=Strict 'Path=/' 'Max-Age=43200'; do
  expect "cookie $attr" "$(grep -c "; $attr" <<<"$set_cookie")" 1
done
# The cookie is Secure; curl will not send it over plain HTTP from a jar,
# so it is passed explicitly.
cookie="$(sed -E 's/^[^:]*: (hookrelay_admin=[^;]*).*/\1/' <<<"$set_cookie")"
session="$(curl_ "$admin/admin/v1/session" -H "Cookie: $cookie")"
csrf="$(json "$session" 'd["csrf_token"]')"
expect "session read" "$(json "$session" 'd["authenticated"], len(d["csrf_token"])')" "(True, 22)"
cookie_call() { # cookie_call <method> <path> [extra curl args...]
  local method="$1" path="$2"
  shift 2
  curl_ -X "$method" "$admin$path" -H "Cookie: $cookie" -H "Origin: $origin" "$@" -w '\n%{http_code}'
}
expect "replay without CSRF" "$(tail -n1 <<<"$(cookie_call POST "/admin/v1/dead-letters/$dl_replay/replay")")" 403
out="$(cookie_call POST "/admin/v1/dead-letters/$dl_replay/replay" -H "X-CSRF-Token: $csrf")"
expect "replay with CSRF" "$(tail -n1 <<<"$out")" 200
expect "cookie replay" "$(json "$(head -n -1 <<<"$out")" 'd["status"], d["delivery_cycle"]')" "('replayed', 2)"
out="$(cookie_call POST "/admin/v1/dead-letters/$dl_delete/payload" -H "X-CSRF-Token: $csrf")"
expect "payload status" "$(tail -n1 <<<"$out")" 200
expect "payload text" "$(json "$(head -n -1 <<<"$out")" 'd["message"]["payload"]["message"]["text"]')" '<img src=x onerror=alert(1)>'
etag="$(curl_ -D - -o /dev/null "$admin/admin/v1/dead-letters/$dl_delete" -H "Cookie: $cookie" | grep -i '^etag:' | tr -d '\r' | cut -d' ' -f2)"
expect "delete without If-Match" "$(tail -n1 <<<"$(cookie_call DELETE "/admin/v1/dead-letters/$dl_delete" -H "X-CSRF-Token: $csrf")")" 428
expect "delete with If-Match" "$(tail -n1 <<<"$(cookie_call DELETE "/admin/v1/dead-letters/$dl_delete" -H "X-CSRF-Token: $csrf" -H "If-Match: $etag")")" 204
expect "deleted message gone" "$(vk EXISTS "hr1:m:$dl_delete" "hr1:dl:$dl_delete")" 0
summary="$(curl_ "$admin/admin/v1/operations/summary" -H "Cookie: $cookie")"
expect "summary via cookie" "$(json "$summary" 'd["dead_letters"]["count"], d["admin_sessions"]["indexed"]')" "(0, 1)"
audit="$(admin_cli audit list --limit 5)"
expect "audit trail" "$(json "$audit" '[(i["operation"], i["actor"]) for i in d["items"][:4]]')" \
  "[('dead_letter_deleted', 'admin_session'), ('dead_letter_payload_viewed', 'admin_session'), ('dead_letter_replayed', 'admin_session'), ('admin_login', 'admin_session')]"
expect "logout" "$(tail -n1 <<<"$(cookie_call DELETE /admin/v1/session -H "X-CSRF-Token: $csrf")")" 204
expect "session after logout" "$(curl_ -o /dev/null -w '%{http_code}' "$admin/admin/v1/session" -H "Cookie: $cookie")" 401

step "Milestone 4: operational UI and metrics"
ui_headers="$(curl_ -D - -o /dev/null "$admin/ui/" | tr -d '\r')"
expect "UI status" "$(head -n1 <<<"$ui_headers" | awk '{print $2}')" 200
expect "UI CSP" "$(grep -ci "^content-security-policy: default-src 'none'; script-src 'self'" <<<"$ui_headers")" 1
expect "root redirect" "$(curl_ -o /dev/null -w '%{http_code} %{redirect_url}' "$admin/")" "302 $admin/ui/"
metrics="$(curl_ "$admin/metrics")"
expect "login success" "$(metric 'hookrelay_admin_login_attempts_total\{outcome="success"\}')" 1
expect "CSRF rejection" "$(metric 'hookrelay_admin_csrf_rejections_total\{reason="token"\}')" 1
expect "payload views" "$(metric 'hookrelay_dlq_payload_inspections_total\{outcome="disclosed"\}')" 1
expect "deletions" "$(metric 'hookrelay_dead_letter_deletions_total\{outcome="deleted"\}')" 1

printf '\nSMOKE PASSED\n'
