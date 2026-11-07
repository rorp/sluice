#!/bin/bash
# eclair + Sluice on testnet3, against Lightning Labs' testnet swap server.
#
# Usage: ./testnet.sh bitcoin-start|bitcoin-stop|sync|bitcoin ...
#        ./testnet.sh eclair-start|eclair-stop|eclair-cli ...
#
# Eclair defaults to the 0.14.6 build unpacked in .data (it ignores malformed gossip that Lightning Labs' node relays)
# and to a plugin jar built against it (mvn package -Declair.version=0.14.6, copied to .data). Override with
# ECLAIR_HOME=<eclair-node dist> and PLUGIN_JAR=<jar>; the jar must be built against the same Eclair version.
set -e -o pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
DATA="$DIR/.data"
mkdir -p "$DATA/bitcoind" "$DATA/eclair"
[ -f "$DATA/rpc-password" ] || (openssl rand -hex 24 > "$DATA/rpc-password" && chmod 600 "$DATA/rpc-password")
export TESTNET_RPC_PASSWORD="$(cat "$DATA/rpc-password")"
COMPOSE="docker compose -p eclair-loop-testnet -f $DIR/docker-compose.yml"

ECLAIR_HOME="${ECLAIR_HOME:-$DATA/eclair-node-0.14.6-3294e70}"
PLUGIN_JAR="${PLUGIN_JAR:-$DATA/sluice-eclair-0.14.6.jar}"
ECLAIR_DATADIR="$DATA/eclair"
ECLAIR_API_PORT=8083
ECLAIR_P2P_PORT=9736
ECLAIR_API_PASSWORD_FILE="$DATA/eclair-api-password"
[ -f "$ECLAIR_API_PASSWORD_FILE" ] || (openssl rand -hex 16 > "$ECLAIR_API_PASSWORD_FILE" && chmod 600 "$ECLAIR_API_PASSWORD_FILE")
ECLAIR_API_PASSWORD="$(cat "$ECLAIR_API_PASSWORD_FILE")"

function bitcoin() {
  docker exec loop-testnet-bitcoind bitcoin-cli -testnet -rpcuser=eclair -rpcpassword="$TESTNET_RPC_PASSWORD" "$@"
}

function bitcoin-start() {
  $COMPOSE up -d
}

function bitcoin-stop() {
  $COMPOSE down
}

function sync() {
  bitcoin getblockchaininfo | jq -c '{chain, blocks, headers, progress: .verificationprogress, ibd: .initialblockdownload, size_gb: (.size_on_disk / 1e9 | floor)}'
}

function eclair-cli() {
  "$ECLAIR_HOME/bin/eclair-cli" -p "$ECLAIR_API_PASSWORD" -a "http://localhost:$ECLAIR_API_PORT" "$@"
}

function eclair-start() {
  [ -x "$ECLAIR_HOME/bin/eclair-node.sh" ] || { echo "ECLAIR_HOME must point to an unpacked eclair-node distribution" >&2; return 1; }
  [ -f "$PLUGIN_JAR" ] || { echo "plugin jar not found, run mvn package first (or set PLUGIN_JAR)" >&2; return 1; }
  [ "$(bitcoin getblockchaininfo | jq .initialblockdownload)" = false ] || { echo "bitcoind is still syncing: $(sync)" >&2; return 1; }
  bitcoin listwallets | jq -e 'index("eclair")' > /dev/null || bitcoin loadwallet eclair > /dev/null 2>&1 || bitcoin createwallet eclair > /dev/null
  cat > "$ECLAIR_DATADIR/eclair.conf" <<CONF
eclair {
  chain = "testnet"
  node-alias = "sluice-test"
  server.port = $ECLAIR_P2P_PORT
  api {
    enabled = true
    binding-ip = "127.0.0.1"
    port = $ECLAIR_API_PORT
    password = "$ECLAIR_API_PASSWORD"
  }
  bitcoind {
    host = "localhost"
    rpcport = 18332
    rpcuser = "eclair"
    rpcpassword = "$TESTNET_RPC_PASSWORD"
    zmqblock = "tcp://127.0.0.1:29332"
    zmqtx = "tcp://127.0.0.1:29333"
    wallet = "eclair"
  }
  // A fresh node has no channels, and eclair only syncs gossip with peers it has channels with or whitelisted ones:
  // we need gossip to find the swap server's node address. This is ACINQ's testnet node (endurance.acinq.co:9735).
  router.sync.whitelist = ["03933884aaf1d6b108397e5efe5c86bcf2d8ca8d2f700eda99db9214fc2712b134"]
  channel.max-htlc-value-in-flight-percent = 90
  // testnet routes to the swap server are unreliable: try more paths before giving up on a payment
  max-payment-attempts = 30
  // sluice: defaults to test.swap.lightning.today:11010 with system TLS on testnet.
  // the user agent of a release build of loopd v0.35.2-beta, as sent for swaps started with the loop cli
  sluice.user-agent = "loopd/v0.35.2-beta/commit=v0.35.2-beta,initiator=loop-cli"
}
CONF
  chmod 600 "$ECLAIR_DATADIR/eclair.conf"
  nohup "$ECLAIR_HOME/bin/eclair-node.sh" "-Declair.datadir=$ECLAIR_DATADIR" "$PLUGIN_JAR" > "$DATA/eclair.out" 2>&1 &
  echo $! > "$DATA/eclair.pid"
  for _ in $(seq 1 60); do eclair-cli getinfo > /dev/null 2>&1 && break; sleep 2; done
  eclair-cli connect --uri=03933884aaf1d6b108397e5efe5c86bcf2d8ca8d2f700eda99db9214fc2712b134@endurance.acinq.co:9735 > /dev/null || true
  echo "eclair: $(eclair-cli getinfo | jq -c '{nodeId, blockHeight}')"
}

function eclair-stop() {
  if [ -f "$DATA/eclair.pid" ]; then
    local pid
    pid="$(cat "$DATA/eclair.pid")"
    kill "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null; do sleep 1; done
    rm -f "$DATA/eclair.pid"
  fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ $# -lt 1 ]]; then
    sed -n '2,6p' "$0"
    exit 1
  fi
  CMD=$1
  shift
  $CMD "$@"
fi
