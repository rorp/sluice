#!/bin/bash
# Regtest environment for Sluice: bitcoind, lndserver + our regtest loop server behind aperture (L402),
# lndclient + loopd as a reference client, and an eclair node running on the host with the Sluice plugin.
#
# Usage: ./regtest.sh start|stop|setup|mine [n]|info|bitcoin ...|lndserver ...|lndclient ...|loop ...
#        ./regtest.sh eclair-start|eclair-stop|eclair-setup|eclair-cli ...
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE="docker compose -p eclair-loop -f $DIR/docker-compose.yml"
DATA="$DIR/.data"

# eclair runs on the host: ECLAIR_HOME is an unpacked eclair-node distribution (eclair-node-<version>-<commit>-bin.zip).
ECLAIR_HOME="${ECLAIR_HOME:-}"
PLUGIN_JAR="${PLUGIN_JAR:-$(ls "$DIR"/../target/sluice_2.13-*.jar 2>/dev/null | grep -v original | head -1)}"
ECLAIR_DATADIR="$DATA/eclair"
ECLAIR_API_PORT=8082
ECLAIR_P2P_PORT=9737
ECLAIR_API_PASSWORD=regtest

function bitcoin() {
  docker exec loop-bitcoind bitcoin-cli -regtest -rpcuser=lightning -rpcpassword=lightning "$@"
}

function lndserver() {
  docker exec loop-lndserver lncli --network regtest "$@"
}

function lndclient() {
  docker exec loop-lndclient lncli --network regtest "$@"
}

function loop() {
  docker exec -i loop-loopclient loop --network regtest "$@"
}

function mine() {
  local n=${1:-6}
  bitcoin -rpcwallet=miner generatetoaddress "$n" "$(bitcoin -rpcwallet=miner getnewaddress)" > /dev/null
}

function waitfor() {
  local what=$1; shift
  for _ in $(seq 1 120); do
    if "$@" > /dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "timeout waiting for $what" >&2
  return 1
}

function synced() {
  "$1" getinfo | jq -e '.synced_to_chain' > /dev/null
}

function start() {
  mkdir -p "$DATA"
  $COMPOSE up -d --build
  waitfor bitcoind bitcoin getblockchaininfo
  waitfor lndserver lndserver getinfo
  waitfor lndclient lndclient getinfo
  setup
}

function stop() {
  eclair-stop || true
  $COMPOSE down --volumes
  rm -rf "$DATA"
}

function write_aperture_config() {
  cat > "$DATA/aperture.yaml" <<EOF
listenaddr: '0.0.0.0:11018'
staticroot: '/root/.aperture/static'
servestatic: false
debuglevel: debug
insecure: false
writetimeout: 0s

servername: aperture
autocert: false

authenticator:
  lndhost: lndserver:10009
  tlspath: /root/.lnd/tls.cert
  macdir: /root/.lnd/data/chain/bitcoin/regtest
  network: regtest

etcd:
  host: 'etcd:2379'
  user:
  password:

services:
  - name: loop
    hostregexp: '^.*$'
    pathregexp: '^/looprpc.*$'
    address: 'loopserver:11009'
    protocol: https
    tlscertpath: /root/loopserver/tls.cert
    price: 1000
    authwhitelistpaths:
      - '^/looprpc.SwapServer/LoopOutTerms.*$'
      - '^/looprpc.SwapServer/LoopOutQuote.*$'
      - '^/looprpc.SwapServer/LoopInTerms.*$'
      - '^/looprpc.SwapServer/LoopInQuote.*$'
EOF
}

function setup() {
  echo "Configuring aperture"
  write_aperture_config
  docker cp "$DATA/aperture.yaml" loop-aperture:/root/.aperture/aperture.yaml
  docker restart loop-aperture > /dev/null

  echo "Creating miner wallet"
  bitcoin createwallet miner > /dev/null
  mine 106

  echo "Funding lnd nodes"
  bitcoin -rpcwallet=miner sendtoaddress "$(lndserver newaddress p2tr | jq -r .address)" 5 > /dev/null
  bitcoin -rpcwallet=miner sendtoaddress "$(lndclient newaddress p2tr | jq -r .address)" 5 > /dev/null
  mine 6
  waitfor "lndserver sync" synced lndserver
  waitfor "lndclient sync" synced lndclient

  echo "Opening channels lndserver <-> lndclient"
  local server_id client_id
  server_id=$(lndserver getinfo | jq -r .identity_pubkey)
  client_id=$(lndclient getinfo | jq -r .identity_pubkey)
  lndserver connect "$client_id@lndclient:9735" > /dev/null
  lndserver openchannel --node_key "$client_id" --local_amt 16000000 > /dev/null
  mine 6
  waitfor "lndclient funds" bash -c "docker exec loop-lndclient lncli --network regtest walletbalance | jq -e '.confirmed_balance | tonumber > 0'"
  lndclient openchannel --node_key "$server_id" --local_amt 16000000 > /dev/null
  mine 6
  waitfor "active channels" bash -c "[ \$(docker exec loop-lndclient lncli --network regtest getinfo | jq .num_active_channels) = 2 ]"

  echo "Restarting loopd"
  docker restart loop-loopclient > /dev/null
  waitfor loopd loop getinfo
  info
}

function info() {
  echo "lndserver: $(lndserver getinfo | jq -c '{pubkey: .identity_pubkey, channels: .num_active_channels, height: .block_height}')"
  echo "lndclient: $(lndclient getinfo | jq -c '{pubkey: .identity_pubkey, channels: .num_active_channels, height: .block_height}')"
}

function eclair-cli() {
  "$ECLAIR_HOME/bin/eclair-cli" -p "$ECLAIR_API_PASSWORD" -a "http://localhost:$ECLAIR_API_PORT" "$@"
}

function eclair-start() {
  [ -x "$ECLAIR_HOME/bin/eclair-node.sh" ] || { echo "ECLAIR_HOME must point to an unpacked eclair-node distribution" >&2; return 1; }
  [ -f "$PLUGIN_JAR" ] || { echo "plugin jar not found, run mvn package first (or set PLUGIN_JAR)" >&2; return 1; }
  mkdir -p "$ECLAIR_DATADIR"
  bitcoin listwallets | jq -e 'index("eclair")' > /dev/null || bitcoin loadwallet eclair > /dev/null 2>&1 || bitcoin createwallet eclair > /dev/null
  docker cp loop-aperture:/root/.aperture/tls.cert "$DATA/aperture-tls.cert"
  cat > "$ECLAIR_DATADIR/eclair.conf" <<EOF
eclair {
  chain = "regtest"
  node-alias = "sluice"
  server.port = $ECLAIR_P2P_PORT
  // Eclair lowers its channels' htlc_maximum_msat when their balance is low, as on mainnet, but by default waits up to
  // an hour before raising it again: tests that refill a channel need routes through it right away.
  channel.channel-update.min-time-between-updates = 1 second
  api {
    enabled = true
    binding-ip = "127.0.0.1"
    port = $ECLAIR_API_PORT
    password = "$ECLAIR_API_PASSWORD"
  }
  bitcoind {
    host = "localhost"
    rpcport = 18443
    rpcuser = "lightning"
    rpcpassword = "lightning"
    zmqblock = "tcp://127.0.0.1:28334"
    zmqtx = "tcp://127.0.0.1:28333"
    wallet = "eclair"
  }
  sluice {
    server = "localhost:11018"
    tls = "file"
    tls-cert-file = "$DATA/aperture-tls.cert"
  }
}
EOF
  nohup "$ECLAIR_HOME/bin/eclair-node.sh" "-Declair.datadir=$ECLAIR_DATADIR" "$PLUGIN_JAR" > "$DATA/eclair.out" 2>&1 &
  echo $! > "$DATA/eclair.pid"
  waitfor eclair eclair-cli getinfo
  # eclair would otherwise reconnect to the address lndserver announces, which isn't reachable from the host.
  local server_id
  server_id=$(lndserver getinfo 2>/dev/null | jq -r .identity_pubkey)
  [ -n "$server_id" ] && eclair-cli connect --uri="$server_id@localhost:9735" > /dev/null 2>&1 || true
  echo "eclair: $(eclair-cli getinfo | jq -c '{nodeId, blockHeight}')"
}

function eclair-stop() {
  if [ -f "$DATA/eclair.pid" ]; then
    kill "$(cat "$DATA/eclair.pid")" 2>/dev/null || true
    rm -f "$DATA/eclair.pid"
  fi
}

# Funds eclair and opens channels in both directions with lndserver (the swap server's node).
function eclair-setup() {
  local server_id eclair_id
  server_id=$(lndserver getinfo | jq -r .identity_pubkey)
  eclair_id=$(eclair-cli getinfo | jq -r .nodeId)
  bitcoin -rpcwallet=miner sendtoaddress "$(bitcoin -rpcwallet=eclair getnewaddress)" 5 > /dev/null
  mine 6
  sleep 5
  eclair-cli connect --uri="$server_id@localhost:9735" > /dev/null
  eclair-cli open --nodeId="$server_id" --fundingSatoshis=16000000 > /dev/null
  mine 6
  lndserver connect "$eclair_id@host.docker.internal:$ECLAIR_P2P_PORT" > /dev/null 2>&1 || true
  waitfor "lndserver sync" synced lndserver
  lndserver openchannel --node_key "$eclair_id" --local_amt 16000000 > /dev/null
  # lnd may broadcast its funding tx a bit later: keep mining until both channels are ready.
  for _ in $(seq 1 60); do
    [ "$(eclair-cli channels | jq '[.[] | select(.state == "NORMAL")] | length')" = 2 ] && break
    mine 1
    sleep 2
  done
  echo "eclair channels: $(eclair-cli channels | jq -c '[.[] | {state, capacity: .data.commitments.active[0].fundingAmount}]')"
}

# The functions above can be sourced (see itest.sh).
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ $# -lt 1 ]]; then
    sed -n '2,6p' "$0"
    exit 1
  fi
  CMD=$1
  shift
  $CMD "$@"
fi
