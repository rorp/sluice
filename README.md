# Sluice

Sluice is a [Lightning Loop](https://github.com/lightninglabs/loop) client plugin for [Eclair](https://github.com/ACINQ/eclair):

- **Loop Out** (off-chain → on-chain): pay the swap server over Lightning, receive the amount on-chain. You get inbound
  liquidity.
- **Loop In** (on-chain → off-chain): lock funds from Eclair's on-chain wallet in an htlc, the server pays you over
  Lightning. You get outbound liquidity.

It is a port of `loopd`'s swap logic (`lightninglabs/loop` at `fd3ed6c`, protocol `MUSIG2`: taproot htlcs with MuSig2
cooperative spends) running inside Eclair, using Eclair's payments, invoices and bitcoind wallet.

**Status: alpha.** Swaps work end-to-end on regtest against a swap server built from loop's own packages
(`regtest/loopserver`), including the failure paths. They have run against Lightning Labs' testnet server, which is
closed source. Not supported: autoloop, static addresses, sweep batching, externally funded loop ins, Tor.

## Build and install

Sluice is built against Eclair 0.14.6, which must be in the local Maven repository:

```sh
git clone https://github.com/ACINQ/eclair.git

cd eclair/

git checkout v0.14.6

mvn install -DskipTests=true
```

Then build the plugin:

```sh
git clone https://github.com/rorp/sluice.git

cd sluice/

mvn package
```

To run it, pass the jar to Eclair, which must use its bitcoind wallet (the plugin refuses to start with another
on-chain wallet):

```sh
eclair-node.sh <path-to>/sluice_2.13-0.1.0-SNAPSHOT.jar
```

Build against another version with `-Declair.version=<version>`.

The jar is shaded: it bundles grpc-java (with shaded netty) and relocated copies of guava and protobuf.

## Configuration

These keys go in `eclair.conf`:

```hocon
eclair.sluice {
  // Defaults to swap.lightning.today:11010 on mainnet and test.swap.lightning.today:11010 on testnet3, required on
  // other chains.
  server = "localhost:11009"
  tls = "system"            // system (trust system CAs) | file (trust tls-cert-file) | insecure (plaintext, testing only)
  tls-cert-file = ""
  user-agent = ""           // defaults to sluice/v<version>/eclair=<eclair version>
  rpc-timeout = 30 seconds
  max-l402-cost-sat = 1000  // we never automatically pay more than this for an L402 token
  max-l402-routing-fee-sat = 10
  l402-payment-timeout = 60 seconds
  l402-min-purchase-interval = 24 hours // we don't automatically replace a rejected L402 token bought more recently
  default-sweep-conf-target = 9
  loop-out-htlc-confirmations = 1 // confirmations of the server's htlc before we reveal the preimage (loopd's default)
  max-swap-fee-ppm = 5000   // swap fee limit when a swap doesn't set maxSwapFeeSat (0.5% of the amount)
  max-prepay-sat = 30000    // prepay limit when a loop out doesn't set maxPrepayAmountSat
}
```

Earlier versions, named eclair-loop, read the `eclair.loop` section: Eclair refuses to start while it is there,
rather than silently ignoring its settings. Rename it to `eclair.sluice`. They also derived swap keys from another
seed: let their swaps finish before switching, since Sluice can't sweep or refund them.

The server requires an [L402](https://docs.lightning.engineering/the-lightning-network/l402) token for swaps: the
first swap call pays for one through Eclair (1 sat on Lightning Labs' servers). It is stored and reused. If the server
rejects it, we only buy a new one automatically if ours is older than `l402-min-purchase-interval`, since our swaps
call the server every block and every minute: use `loopfetchl402` to buy one sooner.

## API

| Command | Parameters | Description |
|---|---|---|
| `loopoutterms` | | Server limits for loop out (amounts, cltv delta) |
| `loopoutquote` | `amountSat`, `confTarget` (default 9), `fast` | Swap fee, prepay amount and htlc expiry |
| `loopout` | `amountSat`, `confTarget`, `fast`, `address`, `maxSwapFeeSat`, `maxPrepayAmountSat`, `maxMinerFeeSat`, `maxSwapRoutingFeeSat`, `maxPrepayRoutingFeeSat`, `htlcConfirmations`, `idempotencyKey` | Starts a loop out to `address` (default: a new Eclair wallet address) |
| `loopinterms` | | Server limits for loop in |
| `loopinquote` | `amountSat`, `lastHop`, `confTarget` (default 6, at most 1008), `routeHints` | Swap fee, cltv delta and estimated htlc fee (the server first probes our node) |
| `loopin` | `amountSat`, `confTarget`, `lastHop`, `address`, `maxSwapFeeSat`, `maxMinerFeeSat`, `routeHints`, `idempotencyKey` | Starts a loop in; `address` receives the refund if the server doesn't pay (default: a new Eclair wallet address) |
| `looplistswaps` | | All swaps and their state |
| `loopswapinfo` | `swapHash` | Details of a swap: htlc, our transactions, fees paid |
| `loopabandonswap` | `swapHash` | Stops a swap when it is safe (see below) |
| `loopfetchl402` | | Buys an L402 token if we don't have one or the server rejects ours (costs up to `max-l402-cost-sat`) |
| `looplistl402` | | Our L402 tokens |

All parameters except `amountSat` and `swapHash` are optional.

`loopout` and `loopin` can take longer than Eclair's API timeout (30 seconds, or `--timeoutSeconds`), for example when
they first buy an L402 token. **A call that timed out may still create the swap.** To retry safely, give each swap
an `idempotencyKey` (any string of up to 64 characters, e.g. a UUID) and retry with the same key and parameters:

- if the first call created a swap, the retry returns it (whatever its state), even after a restart;
- if the first call is still creating it, the retry waits for it;
- if the first call failed before its swap was saved, the retry creates it.

A key can't be used for another request (other parameters, or the other swap type): use a new key for each new swap.
`looplistswaps` shows each swap's key. Without a key, check `looplistswaps` before you try again, or you may move the
amount (and pay the fees) twice.

```sh
eclair-cli loopout --amountSat=500000 --idempotencyKey=payout-2026-11-06
```

```sh
eclair-cli loopoutquote --amountSat=500000
eclair-cli loopout --amountSat=500000
eclair-cli loopswapinfo --swapHash=<hash>
```

Fee limits default to what the `loop` cli uses:

- swap fee and loop out prepay: the quote. Since we don't ask you to confirm it like the `loop` cli does, we refuse
  quotes above `max-swap-fee-ppm` and `max-prepay-sat` (autoloop's defaults): set `maxSwapFeeSat` and
  `maxPrepayAmountSat` to accept them. To enforce exactly what `loopoutquote` or `loopinquote` showed you, pass its
  values: the server could quote differently when the swap starts;
- loop out routing fees: 10 sat + 2% of each invoice;
- loop out miner fee: 250× the estimated fee of our sweep, at most 20% of the swap amount. This looks high on purpose:
  once the preimage is revealed, a sweep that doesn't confirm in time loses the whole swap amount. We refuse a limit
  below the estimated fee of our sweep;
- loop in miner fee: 3× the estimated htlc fee, which caps both the htlc transaction and the refund.

Set `fast=true` for a loop out to ask the server to publish its htlc right away. Otherwise it may wait up to 30
minutes to batch it with other swaps, for a lower fee.

Use `routeHints=true` for a loop in (and its quote) if your node has no public channels: like loopd's `--private`,
we give the server routing hints for our private channels (only those with `lastHop` if set) in its probe and quote
requests and in our invoices. The server can't find a route to you otherwise.

## How swaps run, and what is at stake

Each pending swap runs in its own actor. On every new block, on payment events and every minute it checks the chain
and Eclair's payments, and moves the swap forward. Its progress is persisted before anything is published or sent, so
swaps resume after a restart. We never trust the server for the state of the chain.

If Eclair stops while sending a swap payment, before its htlcs are committed with our peer, Eclair forgets those htlcs
but its database still shows the payment as pending. This includes htlcs that Eclair had added but not signed yet:
it drops them when the channel reconnects. At startup, the plugin marks such payments as failed ("eclair stopped
before the htlc was committed") and pays the invoice again. When only some parts of a multi-part payment were
committed, the payment can't complete: the plugin pays again once the server's node has failed those parts, after
waiting for the rest (2 minutes with lnd). Swap payments are only sent once one of our channels is online: after a
restart, they wait for our peers to reconnect instead of failing right away.

### Loop Out

| State | Meaning |
|---|---|
| `INITIATED` | We paid the swap invoice (held by the server) and the prepay invoice; waiting for the server's htlc |
| `PREIMAGE_REVEALED` | The htlc has `htlcConfirmations` confirmations: we give the preimage to the server and are sweeping the htlc |
| `SUCCESS` | Our sweep has 3 confirmations |
| `FAILED_OFFCHAIN_PAYMENTS` | One of our payments failed; we told the server why (`CancelLoopOutSwap`) |
| `FAILED_TIMEOUT` | The htlc didn't have `htlcConfirmations` confirmations 20 blocks before its expiry, or our sweep would have cost more than `maxMinerFee` until then: we never reveal the preimage |
| `FAILED_INCORRECT_HTLC_AMOUNT` | The server's htlc pays less than the swap amount |
| `FAILED_SWEEP_TIMEOUT` | The server reclaimed its htlc after its expiry (its transaction has 3 confirmations) |
| `FAILED_ABANDONED` | Abandoned by the user |

In the failed states before `PREIMAGE_REVEALED`, the swap payment fails back to us once the server's hold invoice
expires. We lose the prepay (and routing fees) if the server already settled it.

**After `PREIMAGE_REVEALED` the server can take our payment, so our sweep must confirm before the htlc expires.**
Like loopd, we only reveal the preimage if our sweep costs at most `maxMinerFee` at the current feerate: otherwise we
wait for feerates to drop. We sweep cooperatively with the server (MuSig2 key path: cheaper, and the swap looks like
any taproot spend), or alone with the preimage if it doesn't answer. The server only co-signs once it has our preimage:
like loopd, we push it again at every block until the server takes it or settles our payment. Like loopd, we raise its feerate by 100 sat/kw every
block until it confirms (or to the current estimate, if higher), up to `maxMinerFee`. In the last 10 blocks we use an urgent feerate and may pay more than `maxMinerFee`, up to 20% of the
htlc amount (like loopd), since the alternative is losing all of it. Keep Eclair running until the swap succeeds.

A reorg that removes the server's htlc after we revealed the preimage lets the server take our payment and double-spend
its htlc. We sweep any other htlc that the server publishes for our swap, but for large swaps you should wait for more
confirmations before revealing the preimage, with `htlcConfirmations` (`loop-out-htlc-confirmations` by default).

### Loop In

| State | Meaning |
|---|---|
| `INITIATED` | Creating our htlc transaction |
| `HTLC_PUBLISHED` | Our htlc is published: waiting for the server to pay our invoice (or refunding it after its expiry) |
| `INVOICE_SETTLED` | The server paid us; we gave it our MuSig2 internal key so it can sweep cheaply |
| `SUCCESS` | The server swept the htlc, or it expired after paying us |
| `FAILED_TIMEOUT` | The server didn't pay us in time: our refund has 3 confirmations (or our htlc expired before it confirmed, and was double-spent) |
| `FAILED_INSUFFICIENT_FUNDS` | Eclair's wallet couldn't fund the htlc within `maxMinerFee` |
| `FAILED_ABANDONED` | Abandoned by the user before the htlc was published, or our invoice was deleted (with Eclair's `deleteinvoice`) before it was: the server couldn't pay us anymore |

If the server doesn't pay, our funds stay locked in the htlc until its expiry (about 1000 blocks, a week). Then we
delete our invoice (Eclair refuses once it has been paid, so a late payment and a refund can't both happen) and spend
the htlc back to our wallet.

If our htlc doesn't confirm before its expiry (its feerate was too low for example), we keep waiting for it like loopd:
Eclair's wallet and other nodes may broadcast it again, and we refund it if it confirms. The swap only fails once one of
the htlc transaction's inputs is spent by another confirmed transaction: to get your funds back sooner, spend one of
them to your own wallet. Don't fee-bump the htlc transaction instead: we only watch the transaction we published.

### Abandoning a swap

`loopabandonswap` only stops swaps that can be stopped safely:

- a loop out before the preimage is revealed: we stop paying attention to the server's htlc, and our swap payment will
  fail back to us (the prepay is probably lost);
- a loop in before its htlc is published: our invoice is deleted.

Other swaps can't be abandoned: they must sweep or refund their htlc.

### Backups and recovery

- Our swap keys are derived from Eclair's node key, from a seed computed as HMAC-SHA256(node key, "sluice-seed"):
  `m/0'/i'` for htlc keys, and HMAC-SHA256(`m/2'/i'`, swap hash) for loop in internal keys. Backing up Eclair's seed covers them. We give loop in internal keys to the server once
  it paid us, which is why they depend on the swap hash: key indexes start over if the plugin's database is lost, but
  we never give the server the internal key of another swap.
- **Preimages and swap data are only in the plugin's database**: `<datadir>/<chain>/loop.sqlite`, or the `loop`
  schema of Eclair's Postgres database. Back it up like Eclair's own database, and never delete it while swaps are
  pending: a loop out whose preimage is lost can't be swept, and a loop in's refund needs its htlc details.
- The plugin's database schema is at version 2 (idempotency keys): it is upgraded at startup, and older versions of the
  plugin refuse to open it.
- If you switch Eclair from SQLite to Postgres, the plugin copies `loop.sqlite` into the `loop` schema at startup, and
  marks `loop.sqlite` as migrated (it creates an empty, marked `loop.sqlite` when Eclair always used Postgres). Eclair
  then refuses to start in two cases, rather than running without some of our swaps: if the `loop` schema already has
  other swaps (merge them manually), or if Eclair switches back to SQLite (delete the marked `loop.sqlite` once no swap
  in Postgres is pending). L402 tokens are always merged, since they hold no funds.

### Monitoring

The plugin logs each state transition, every transaction it publishes and every server error (look for
`LoopOutProcessor`, `LoopInProcessor` and `SwapActor` in `eclair.log`). Pending swaps are listed by `looplistswaps`.
A database error ("error while processing") doesn't stop a swap: it is retried at the next block, from the state the
database has.

## Testing

```sh
mvn test   # unit tests: htlc vectors generated with loop's Go code, swap state machines against a fake server and chain
```

`regtest/` has a docker compose environment: bitcoind, `lndserver` with a regtest swap server behind aperture, and
`lndclient` with `loopd` as a reference client. Eclair runs on the host with this plugin:

```bash
mvn package
cd regtest
./regtest.sh start && ./regtest.sh setup            # builds images, mines, funds, opens lndserver <-> lndclient channels
export ECLAIR_HOME=/path/to/eclair-node-<version>-bin
./regtest.sh eclair-start && ./regtest.sh eclair-setup
./itest.sh                                          # integration tests (or ./itest.sh loop_in_refund ...)
./regtest.sh eclair-cli loopout --amountSat=500000 --fast=true   # manual swaps: then ./regtest.sh mine 1
./regtest.sh loop out --amt 500000 --fast --force   # reference swap with loopd
./regtest.sh stop                                   # removes containers, volumes and eclair data
```

`itest.sh` runs real swaps through every path:

- loop out: cooperative sweep, script-path sweep when the server refuses to co-sign, Eclair restart in the middle of
  a payment, server down after publishing its htlc, a payment failure reported with `CancelLoopOutSwap`, a reorg
  after the preimage is revealed, and abandon;
- loop in: key-path sweep by the server with our pushed internal key, Eclair restart, and a refund after the server
  disappeared.

It checks that Eclair's channels are still open at the end.

The official regtest loop server image isn't published anymore: `regtest/loopserver` is a minimal replacement written
with loop's own packages (v3 taproot htlcs, MuSig2 via lnd's code, in-memory state). It is a test tool only, and
encodes our reading of the protocol: `loopd` swaps against it are the reference. Switches (environment variables for
`docker compose`): `LOOPSERVER_NO_COOP_SWEEP=true` refuses MuSig2 co-signing, and `LOOPSERVER_LOOPOUT_CLTV_EXTRA=3000`
creates loop out invoices that Eclair can't pay.

## Notes

- `src/main/protobuf` contains the swap server protos from `lightninglabs/loop` at `fd3ed6c` (MIT, see `LICENSE.loop`),
  with a `java_package` option added.
- The server's update streams aren't used: swaps only rely on the chain and on our payments.
- Quotes don't include our miner fees, except `loopinquote`'s estimated htlc fee.
