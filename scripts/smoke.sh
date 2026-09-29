#!/usr/bin/env bash
# Compose smoke test: the Milestone 1 vertical slice end to end against the
# pinned Compose stack, in production mode (AOF + noeviction gate).
#
#   start stack → create Telegram endpoint (CLI) → send signed fixture →
#   repeat and prove exactly one stored message → claim (wait_ms=0) → ack →
#   repeat ack and receive the recorded result → prove the queue empty →
#   check metrics and health → restart keeping the Valkey volume → verify
#   readiness, the persisted endpoint, and continued deduplication.
#
# The run uses its own Compose project, generated secrets, and free loopback
# ports, so it never touches a developer's .secrets/ or running stack.
# Usage: scripts/smoke.sh            (builds the image first)
#        SMOKE_KEEP=1 scripts/smoke.sh (leave the stack up on failure)
set -euo pipefail

cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME="hookrelay-smoke-$$"
work="$(mktemp -d)"
mkdir -m 0700 "$work/secrets"

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }
public_port="$(free_port)"
admin_port="$(free_port)"

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
claim() {
  curl_ -X POST "$public/v1/deliveries/claim" -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $consumer_secret" -d "{\"operation_id\":\"$1\",\"wait_ms\":0}" -w '\n%{http_code}'
}
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
expect "live" "$(curl_ -o /dev/null -w '%{http_code}' "$admin/health/live")" 200
expect "accepting webhooks" "$(curl_ -o /dev/null -w '%{http_code}' "$admin/health/accepting-webhooks")" 200

step "restart the stack keeping the Valkey volume"
compose down
compose up -d --wait
wait_ready
compose exec -T hookrelay /hookrelay admin webhook get --type telegram --identifier wh_smoke --output json >"$work/got.json"
expect "persisted endpoint" "$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["bot_id"], d["enabled"])' "$work/got.json")" "424242 True"
expect "webhook after restart" "$(send_webhook)" 200
expect "stored messages after repeat" "$(vk --scan --pattern 'hr1:m:*' | wc -l | tr -d ' ')" 0
metrics="$(curl_ "$admin/metrics")"
expect "deduplicated after restart" "$(metric 'hookrelay_messages_duplicate_total\{webhook_type="telegram"\}')" 1
expect "reconciliation findings" "$(grep -c '^hookrelay_reconciliation_findings_total' <<<"$metrics" || true)" 0

printf '\nSMOKE PASSED\n'
