#!/bin/bash
# Integration tests of Sluice against the regtest swap server: real eclair, bitcoind, lnd, aperture and loop server.
#
# Requires a running environment: ./regtest.sh start && ./regtest.sh setup && ECLAIR_HOME=... ./regtest.sh eclair-start
# && ./regtest.sh eclair-setup (with the plugin jar built: mvn package).
#
# Usage: ECLAIR_HOME=<eclair-node dist> ./itest.sh [test names...]   (all tests by default)
set -o pipefail

# shellcheck source=regtest.sh
source "$(cd "$(dirname "$0")" && pwd)/regtest.sh"
set +e

PASSED=()
FAILED=()

function log() {
  echo "[$(date +%H:%M:%S)] $*"
}

function api() {
  local cmd=$1
  shift
  local args=()
  for a in "$@"; do args+=(-F "$a"); done
  curl -s -u ":$ECLAIR_API_PASSWORD" -X POST "http://localhost:$ECLAIR_API_PORT/$cmd" "${args[@]}"
}

function state() {
  api loopswapinfo "swapHash=$1" | jq -r .state
}

function check() {
  local description=$1
  shift
  if "$@"; then
    log "  ok: $description"
  else
    log "  FAILED: $description"
    return 1
  fi
}

# Mines one block at a time until the swap reaches the expected state (or another final state), up to max_blocks.
function wait_state() {
  local hash=$1 expected=$2 max_blocks=${3:-20} s
  for _ in $(seq 0 "$max_blocks"); do
    sleep 4
    s=$(state "$hash")
    [ "$s" = "$expected" ] && return 0
    case "$s" in SUCCESS | FAILED_*) log "  swap $hash is $s, expected $expected"; return 1 ;; esac
    mine 1
  done
  log "  swap $hash is still $s, expected $expected"
  return 1
}

# Waits (without mining) until the predicate succeeds, up to n seconds.
function wait_until() {
  local n=$1
  shift
  for _ in $(seq 1 "$n"); do
    "$@" && return 0
    sleep 1
  done
  return 1
}

function server_logged() {
  docker logs loop-loopserver 2>&1 | grep -q "$1"
}

# Restarts the loop server with the given environment (e.g. LOOPSERVER_NO_COOP_SWEEP=true): it loses its swaps.
function restart_server() {
  env "$@" $COMPOSE up -d --force-recreate loopserver > /dev/null 2>&1
  wait_until 60 server_logged "swap server listening"
  docker restart loop-aperture > /dev/null
  sleep 3
}

function sweep_witness_size() {
  local txid
  txid=$(api loopswapinfo "swapHash=$1" | jq -r '.loopOut.sweepTxIds[-1]')
  bitcoin getrawtransaction "$txid" true | jq '.vin[0].txinwitness | length'
}

# A swap whose server state was lost: we settle its swap invoice with the preimage revealed by our sweep, so that lnd
# doesn't keep our payment pending.
function settle_server_invoice() {
  local txid preimage
  txid=$(api loopswapinfo "swapHash=$1" | jq -r '.loopOut.sweepTxIds[-1]')
  preimage=$(bitcoin getrawtransaction "$txid" true | jq -r '.vin[0].txinwitness[0]')
  lndserver settleinvoice "$preimage" > /dev/null 2>&1 || true
}

# Prints the new swap's hash, or logs the api error and fails.
function new_swap() {
  local res
  res=$(api "$@")
  if jq -e .swapHash <<< "$res" > /dev/null 2>&1; then
    jq -r .swapHash <<< "$res"
  else
    log "  $1 failed: $res" >&2
    return 1
  fi
}

function loop_out() {
  new_swap loopout "$@"
}

function loop_in() {
  new_swap loopin "$@"
}

# Succeeds if the api call fails with an error containing the given message.
function api_fails_with() {
  local msg=$1
  shift
  api "$@" | jq -r .error | grep -qF "$msg"
}

# The short channel ids of the channels we sent payments through since the given unix time.
function sent_through() {
  local ids
  ids=$(api audit "from=$1" | jq -c '[.sent[].parts[].channelId] | unique')
  api channels | jq -r --argjson ids "$ids" '[.[] | select(.channelId as $c | $ids | index($c)) | .data.lastAnnouncement_opt.shortChannelId] | sort | join(",")'
}

function sent_count() {
  api audit "from=$1" | jq '.sent | length'
}

function channels_normal() {
  [ "$(api channels | jq '[.[] | select(.state == "NORMAL")] | length')" = 2 ]
}

function reconnect_eclair() {
  api connect "uri=$(lndserver getinfo | jq -r .identity_pubkey)@localhost:9735" > /dev/null
  wait_until 30 channels_normal
}

# --- Tests ---

function test_loop_out_cooperative() {
  local h
  h=$(loop_out amountSat=500000 fast=true)
  wait_state "$h" SUCCESS &&
    check "key path sweep (1 witness item)" [ "$(sweep_witness_size "$h")" = 1 ] &&
    check "server saw a key path spend" server_logged "swap $h: htlc claimed by client (key path)" &&
    check "fees reported" [ "$(api loopswapinfo "swapHash=$h" | jq '.loopOut.minerFee > 0')" = true ]
}

function test_loop_out_script_path() {
  local h ok
  restart_server LOOPSERVER_NO_COOP_SWEEP=true
  h=$(loop_out amountSat=400000 fast=true)
  wait_state "$h" SUCCESS &&
    check "script path sweep (4 witness items)" [ "$(sweep_witness_size "$h")" = 4 ] &&
    check "server saw a script path spend" server_logged "swap $h: htlc claimed by client (script path)"
  ok=$?
  restart_server
  return $ok
}

function test_loop_out_eclair_restart() {
  local h
  h=$(loop_out amountSat=450000 fast=true)
  eclair-stop
  sleep 5
  mine 2
  eclair-start > /dev/null
  sleep 10
  # If eclair stopped after committing some parts of a payment but not others, the server's node fails those it
  # received after 2 minutes (about 30 blocks here): we then pay again, before the server gives up (after 53 blocks).
  check "eclair reconnected" wait_until 30 channels_normal &&
    wait_state "$h" SUCCESS 50 &&
    check "swap invoice paid once" [ "$(api getsentinfo "paymentHash=$h" | jq '[.[] | select(.status.type == "sent") | .parentId] | unique | length')" = 1 ]
}

function test_loop_out_server_down() {
  local h ok
  h=$(loop_out amountSat=350000 fast=true)
  check "server published its htlc" wait_until 60 server_logged "swap $h: published htlc" || return 1
  docker stop loop-loopserver > /dev/null
  # We can't push the preimage nor sweep cooperatively: we must sweep alone.
  wait_state "$h" SUCCESS &&
    check "script path sweep (4 witness items)" [ "$(sweep_witness_size "$h")" = 4 ]
  ok=$?
  restart_server
  settle_server_invoice "$h"
  return $ok
}

function test_loop_out_payment_failure() {
  local h ok
  # The swap invoice's final cltv delta is too large for eclair: our payment fails without reaching the server.
  restart_server LOOPSERVER_LOOPOUT_CLTV_EXTRA=3000
  h=$(loop_out amountSat=300000 fast=true)
  check "swap failed" wait_until 60 bash -c "[ \"\$(curl -s -u :$ECLAIR_API_PASSWORD -X POST http://localhost:$ECLAIR_API_PORT/loopswapinfo -F swapHash=$h | jq -r .state)\" = FAILED_OFFCHAIN_PAYMENTS ]" &&
    check "server was told why" wait_until 30 server_logged "CancelLoopOutSwap hash=$h route=INVOICE_ROUTE failure=LND_FAILURE_REASON_NO_ROUTE" &&
    check "server cancelled the swap" server_logged "swap $h: SERVER_INITIATED -> SERVER_CLIENT_INVOICE_CANCEL"
  ok=$?
  restart_server
  return $ok
}

function test_loop_out_reorg() {
  local h block
  h=$(loop_out amountSat=550000 fast=true)
  check "server published its htlc" wait_until 60 server_logged "swap $h: published htlc" || return 1
  mine 1
  check "sweep published" wait_until 60 bash -c "[ \"\$(curl -s -u :$ECLAIR_API_PASSWORD -X POST http://localhost:$ECLAIR_API_PORT/loopswapinfo -F swapHash=$h | jq '.loopOut.sweepTxIds | length')\" -ge 1 ]" || return 1
  # The block containing the htlc is reorganized out after we revealed the preimage: the htlc and our sweep go back
  # to the mempool and confirm again in another block.
  block=$(bitcoin getbestblockhash)
  bitcoin invalidateblock "$block"
  mine 1
  sleep 4
  check "still sweeping" [ "$(state "$h")" = PREIMAGE_REVEALED ] &&
    wait_state "$h" SUCCESS
}

function test_loop_out_reorg_before_reveal() {
  local h empty
  h=$(loop_out amountSat=300000 fast=true)
  check "server published its htlc" wait_until 60 server_logged "swap $h: published htlc" || return 1
  # We scan a block without the htlc, which is then replaced by a block (at the same height) containing it: we must
  # scan that height again when the next block arrives, or we never find the htlc.
  empty=$(bitcoin -rpcwallet=miner generateblock "$(bitcoin -rpcwallet=miner getnewaddress)" '[]' | jq -r .hash)
  sleep 4
  bitcoin invalidateblock "$empty"
  mine 1
  sleep 4
  check "htlc not found before the next block" [ "$(state "$h")" = INITIATED ] || return 1
  mine 1
  check "blocks scanned again after the reorg" wait_until 30 grep -q "swap $h: block .* was reorganized out" "$ECLAIR_DATADIR/eclair.log" &&
    wait_state "$h" SUCCESS
}

function test_loop_out_abandon() {
  local h
  h=$(loop_out amountSat=300000 fast=true)
  check "abandoned before the preimage is revealed" [ "$(api loopabandonswap "swapHash=$h" | jq -r .state)" = FAILED_ABANDONED ] &&
    check "final swaps can't be abandoned" bash -c "curl -s -u :$ECLAIR_API_PASSWORD -X POST http://localhost:$ECLAIR_API_PORT/loopabandonswap -F swapHash=$h | grep -q 'already final'" &&
    {
      mine 2
      sleep 8
      check "no sweep" [ "$(api loopswapinfo "swapHash=$h" | jq '.loopOut.sweepTxIds | length')" = 0 ]
    }
  local ok=$?
  # The server would cancel our payment once it reclaims its htlc: we don't wait for that.
  lndserver cancelinvoice "$h" > /dev/null 2>&1 || true
  return $ok
}

# How much eclair can send through the channel with this real scid (msat).
function can_send() {
  api usablebalances | jq --arg s "$1" '[.[] | select(.realScid == $s) | .canSend] | first // 0'
}

# Makes sure that eclair can send at least amount sat through the channel with this real scid: lndserver pays eclair
# through it if needed (loop outs drain eclair's channels).
function fund_channel() {
  local scid=$1 amount=$2 can_send invoice block tx out
  IFS=x read -r block tx out <<< "$scid"
  # A channel that lndserver opened starts with nothing on our side: the first payment also has to cover our channel
  # reserve before we can send anything, so we pay again for what is still missing.
  for _ in 1 2 3; do
    can_send=$(can_send "$scid")
    [ "$can_send" -ge $((amount * 1000)) ] && return 0
    invoice=$(api createinvoice "amountMsat=$((amount * 1000 - can_send + 100000000))" "description=refill $scid" | jq -r .serialized)
    lndserver payinvoice --force --outgoing_chan_id $(((block << 40) | (tx << 16) | out)) --pay_req "$invoice" > /dev/null 2>&1
  done
  [ "$(can_send "$scid")" -ge $((amount * 1000)) ]
}

# Like fund_channel, and waits until eclair's router finds a route for amount sat through the channel: eclair first
# announces a higher htlc_maximum_msat for it (balance-thresholds, without delay on regtest: see regtest.sh).
function refill_channel() {
  fund_channel "$1" "$2" && wait_until 30 route_through "$1" "$2"
}

# Succeeds if eclair's router finds a route to lndserver for amount sat through the channel with this real scid.
function route_through() {
  api findroutetonode "nodeId=$(lndserver getinfo | jq -r .identity_pubkey)" "amountMsat=$(($2 * 1000))" "firstHop=$1" | jq -e '.routes | length > 0' > /dev/null
}

# eclair has two channels with the server's node: we loop out through each of them, by real scid and by alias.
function test_loop_out_first_hop() {
  local channels scid1 scid2 alias2 start h
  channels=$(api channels | jq -c '[.[] | {scid: .data.lastAnnouncement_opt.shortChannelId, alias: .data.aliases.localAlias}]')
  scid1=$(jq -r '.[0].scid' <<< "$channels")
  scid2=$(jq -r '.[1].scid' <<< "$channels")
  alias2=$(jq -r '.[1].alias' <<< "$channels")
  check "eclair can send 400000 sat through $scid1" refill_channel "$scid1" 400000 &&
    check "eclair can send 400000 sat through $scid2" refill_channel "$scid2" 400000 &&
    check "an unknown first hop is refused" api_fails_with "first hop 1x2x3 isn't one of our open channels" loopout amountSat=500000 firstHop=1x2x3 &&
    check "a first hop that can't send the swap amount is refused" api_fails_with "our channel $scid1 can only send" loopout "amountSat=$(($(can_send "$scid1") / 1000))" "firstHop=$scid1" || return 1
  start=$(date +%s)
  h=$(loop_out amountSat=250000 fast=true "firstHop=$scid1") &&
    check "first hop $scid1 in swap info" [ "$(api loopswapinfo "swapHash=$h" | jq -r .loopOut.firstHop)" = "$scid1" ] &&
    wait_state "$h" SUCCESS &&
    check "swap and prepay paid through $scid1 only" [ "$(sent_through "$start")" = "$scid1" ] &&
    check "two payments sent" [ "$(sent_count "$start")" = 2 ] || return 1
  sleep 1
  start=$(date +%s)
  h=$(loop_out amountSat=250000 fast=true "firstHop=$alias2") &&
    check "alias $alias2 of a public channel replaced by its scid $scid2" [ "$(api loopswapinfo "swapHash=$h" | jq -r .loopOut.firstHop)" = "$scid2" ] &&
    wait_state "$h" SUCCESS &&
    check "swap and prepay paid through $scid2 only" [ "$(sent_through "$start")" = "$scid2" ] &&
    check "two payments sent" [ "$(sent_count "$start")" = 2 ]
}

function test_loop_in_key_path() {
  local h
  h=$(loop_in amountSat=600000)
  wait_state "$h" SUCCESS 10 &&
    check "invoice paid" [ "$(api loopswapinfo "swapHash=$h" | jq '.loopIn.amountReceived')" = $(((600000 - $(api loopswapinfo "swapHash=$h" | jq '.loopIn.swapFee')) * 1000)) ] &&
    check "server swept through the key path with our internal key" server_logged "swap $h: swept htlc in .* (key path)"
}

function test_loop_in_eclair_restart() {
  local h
  h=$(loop_in amountSat=500000)
  check "htlc published" wait_until 30 bash -c "[ \"\$(curl -s -u :$ECLAIR_API_PASSWORD -X POST http://localhost:$ECLAIR_API_PORT/loopswapinfo -F swapHash=$h | jq -r .state)\" = HTLC_PUBLISHED ]" || return 1
  eclair-stop
  sleep 5
  eclair-start > /dev/null
  sleep 10
  check "eclair reconnected" wait_until 30 channels_normal &&
    wait_state "$h" SUCCESS 10
}

function test_loop_in_refund() {
  local h expiry ok
  h=$(loop_in amountSat=700000)
  check "htlc published" wait_until 30 bash -c "[ \"\$(curl -s -u :$ECLAIR_API_PASSWORD -X POST http://localhost:$ECLAIR_API_PORT/loopswapinfo -F swapHash=$h | jq -r .state)\" = HTLC_PUBLISHED ]" || return 1
  # The server goes away (and forgets the swap): it will never pay us.
  docker stop loop-loopserver > /dev/null
  expiry=$(api loopswapinfo "swapHash=$h" | jq .cltvExpiry)
  mine $((expiry - $(bitcoin getblockcount) - 1))
  sleep 15
  check "no refund before expiry" [ "$(api loopswapinfo "swapHash=$h" | jq '.loopIn.refundTxIds | length')" = 0 ] &&
    wait_state "$h" FAILED_TIMEOUT 10 &&
    check "invoice deleted" bash -c "curl -s -u :$ECLAIR_API_PASSWORD -X POST http://localhost:$ECLAIR_API_PORT/getreceivedinfo -F paymentHash=$h | grep -q 'Not found'" &&
    check "refund paid to our wallet" [ "$(bitcoin -rpcwallet=eclair gettransaction "$(api loopswapinfo "swapHash=$h" | jq -r '.loopIn.refundTxIds[-1]')" | jq .confirmations)" -ge 3 ]
  ok=$?
  restart_server
  reconnect_eclair
  return $ok
}

function test_api_errors() {
  check "a swap above our fee limit is refused with the reason" bash -c "curl -s -u :$ECLAIR_API_PASSWORD -X POST http://localhost:$ECLAIR_API_PORT/loopout -F amountSat=500000 -F maxSwapFeeSat=1 | grep -q 'exceeds maximum 1 sat'" &&
    check "server errors are reported" bash -c "curl -s -u :$ECLAIR_API_PASSWORD -X POST http://localhost:$ECLAIR_API_PORT/loopout -F amountSat=100000000 | grep -q 'swap amount too high'"
}

function test_no_channel_closed() {
  check "eclair channels are still normal" channels_normal &&
    check "no pending swaps" [ "$(api looplistswaps | jq '[.[] | select(.state | test("SUCCESS|FAILED_") | not)] | length')" = 0 ]
}

ALL_TESTS=(
  loop_out_cooperative
  loop_out_script_path
  loop_out_eclair_restart
  loop_out_server_down
  loop_out_payment_failure
  loop_out_reorg
  loop_out_reorg_before_reveal
  loop_out_abandon
  loop_out_first_hop
  api_errors
  loop_in_key_path
  loop_in_eclair_restart
  loop_in_refund
  no_channel_closed
)

eclair-cli getinfo > /dev/null 2>&1 || { echo "eclair isn't running (see the usage at the top of this file)" >&2; exit 1; }
channels_normal || reconnect_eclair || { echo "eclair's channels with lndserver aren't ready" >&2; exit 1; }
# Loop outs drain eclair's channels by about 3.6M sat per run, more than loop ins bring back: we start each run with
# enough to send on both channels.
for scid in $(api channels | jq -r '.[].data.lastAnnouncement_opt.shortChannelId'); do
  fund_channel "$scid" 2500000 || { echo "cannot fund eclair's channel $scid" >&2; exit 1; }
done

TESTS=("$@")
[ ${#TESTS[@]} -eq 0 ] && TESTS=("${ALL_TESTS[@]}")
for t in "${TESTS[@]}"; do
  log "=== $t"
  if "test_$t"; then PASSED+=("$t"); else FAILED+=("$t"); fi
done

echo
echo "passed: ${#PASSED[@]} (${PASSED[*]})"
echo "failed: ${#FAILED[@]} (${FAILED[*]})"
[ ${#FAILED[@]} -eq 0 ]
