package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.{Block, ByteVector32, Satoshi, SatoshiLong, Transaction, TxIn, TxOut}
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import fr.acinq.eclair.payment.{Bolt11Invoice, PaymentFailed}
import io.github.rorp.sluice.LoopOut.States
import io.github.rorp.sluice.SwapManager.LoopOutRequest
import io.github.rorp.sluice.db.{LoopDb, SqliteLoopDb, SwapType}
import fr.acinq.eclair.transactions.Transactions
import io.github.rorp.sluice.swapserverrpc.{CancelLoopOutSwapRequest, MuSig2SignSweepReq, PaymentFailureReason, RoutePaymentType, ServerLoopOutPushPreimageRequest}
import scodec.bits.ByteVector
import scala.jdk.CollectionConverters.ListHasAsScala
import fr.acinq.eclair.{Alias, BlockHeight, MilliSatoshi, MilliSatoshiLong, RealShortChannelId, ShortChannelId, TimestampMilli, randomBytes32, randomKey}
import akka.pattern.AskTimeoutException
import io.grpc.{Server, Status}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.funsuite.AnyFunSuite
import ch.qos.logback.{classic => logback}

import java.sql.DriverManager
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

class LoopOutSpec extends AnyFunSuite {

  implicit val ec: ExecutionContext = ExecutionContext.global

  private val config = SluiceConfig("in-process", ServerTls.Insecure, "sluice/v0.1.0-test", 10 seconds, maxL402Cost = 1000 sat, maxL402RoutingFee = 10 sat, l402PaymentTimeout = 10 seconds, l402MinPurchaseInterval = 1 hour, defaultSweepConfTarget = 9, loopOutHtlcConfirmations = 1, maxSwapFeePpm = 20_000, maxPrepay = 30_000 sat)

  case class Fixture(server: FakeSwapServer, chain: FakeLoopChain, payments: FakeLoopPayments, db: FlakyLoopDb, manager: SwapManager, env: SwapEnv) {
    @volatile var spawned = Seq.empty[LoopOut.Swap]

    def processor(feerateStep: FeeratePerKw = LoopOut.SweepFeerateStep): LoopOutProcessor = new LoopOutProcessor(env, feerateStep)

    def swap(): LoopOut.Swap = LoopOut.Swap(db.getSwap(spawned.head.swapHash).get)

    def htlc(): Htlc = processor().htlc(swap())
  }

  def withFixture(test: Fixture => Unit): Unit = withConfig(config)(test)

  def withConfig(config: SluiceConfig)(test: Fixture => Unit): Unit = {
    val server = new FakeSwapServer()
    server.loopOutCltvDeltas = (100, 400)
    val name = UUID.randomUUID().toString
    val grpcServer: Server = InProcessServerBuilder.forName(name).directExecutor().addService(server.serviceDefinition).build().start()
    val channel = InProcessChannelBuilder.forName(name).directExecutor().build()
    val db = new FlakyLoopDb(new SqliteLoopDb(DriverManager.getConnection("jdbc:sqlite::memory:")))
    val l402 = new L402Authenticator(db, new FakeL402Payer(server), Block.RegtestGenesisBlock.hash, config.maxL402Cost, config.maxL402RoutingFee, config.l402PaymentTimeout, config.l402MinPurchaseInterval)
    val client = new SwapServerClient(channel, l402, config.userAgent, config.rpcTimeout)
    val chain = new FakeLoopChain(BlockHeight(800_000))
    val payments = new FakeLoopPayments()
    val keys = new LoopKeys(randomBytes32().bytes)
    val env = SwapEnv(client, chain, payments, db, keys, Block.RegtestGenesisBlock.hash)
    var fixture: Fixture = null
    val manager = new SwapManager(client, db, config, randomKey().publicKey, chain, payments, keys, Block.RegtestGenesisBlock.hash, {
      case swap: LoopOut.Swap => fixture.spawned = fixture.spawned :+ swap
      case _ => ()
    })
    fixture = Fixture(server, chain, payments, db, manager, env)
    try {
      test(fixture)
    } finally {
      client.close()
      grpcServer.shutdownNow()
      db.close()
    }
  }

  private def await[T](f: Future[T]): T = Await.result(f, 10 seconds)

  private def awaitFailure(f: Future[_]): Throwable = Await.ready(f, 10 seconds).value.get.failed.get

  /** Creates a swap, runs the first step (paying the invoices) and returns the processor. */
  private def startSwap(f: Fixture, request: LoopOutRequest = LoopOutRequest(1_000_000 sat), feerateStep: FeeratePerKw = LoopOut.SweepFeerateStep): LoopOutProcessor = {
    val info = await(f.manager.loopOut(request))
    assert(info.state == States.Initiated)
    assert(f.spawned.map(_.swapHash) == Seq(info.swapHash))
    val processor = f.processor(feerateStep)
    assert(await(processor.step(f.swap())).state == States.Initiated)
    processor
  }

  private def publishHtlc(f: Fixture, amount: fr.acinq.bitcoin.scalacompat.Satoshi = 1_000_000 sat): Transaction = {
    val tx = FakeLoopChain.fundingTx(f.htlc().pubkeyScriptBytes, amount)
    f.chain.mine(tx)
    tx
  }

  private def fee(f: Fixture, sweep: Transaction): fr.acinq.bitcoin.scalacompat.Satoshi = f.swap().progress.htlc_opt.get.txOut.amount - sweep.txOut.head.amount

  test("create loop out swap") {
    withFixture { f =>
      val info = await(f.manager.loopOut(LoopOutRequest(1_000_000 sat, confTarget_opt = Some(6))))
      assert(info.swapType == SwapType.LoopOut.name)
      assert(info.amount == 1_000_000.sat)
      // conf target below the server's min delta: we use the min delta.
      assert(info.cltvExpiry == BlockHeight(800_100) + SwapManager.LoopOutExpiryMargin)
      val details = info.loopOut.get
      assert(details.swapFee == 10_000.sat && details.prepayAmount == 1337.sat)
      assert(details.limits.maxSwapFee == 10_000.sat)
      assert(details.limits.maxSwapRoutingFee == 20_010.sat)
      assert(details.limits.maxPrepayRoutingFee == 36.sat)
      // 250 times our sweep fee would be above our max share of the swap amount.
      assert(details.limits.maxMinerFee == LoopOut.maxFeeToAmount(1_000_000 sat))
      assert(details.htlcConfirmations == 1)
      assert(details.htlcOutPoint.isEmpty && details.sweepTxIds.isEmpty)
      val record = f.db.getSwap(info.swapHash).get
      assert(record.keyIndex == 0)
      assert(fr.acinq.bitcoin.scalacompat.Crypto.sha256(record.preimage) == info.swapHash)
      // The server got our key and the requested expiry.
      val serverSwap = f.server.loopOuts(info.swapHash)
      assert(serverSwap.htlc == f.htlc())
      assert(details.htlcAddress == f.htlc().address(Block.RegtestGenesisBlock.hash))
      // A second swap uses another key.
      val info2 = await(f.manager.loopOut(LoopOutRequest(500_000 sat)))
      assert(f.db.getSwap(info2.swapHash).get.keyIndex == 1)
      assert(f.manager.listSwaps().map(_.swapHash).toSet == Set(info.swapHash, info2.swapHash))
    }
  }

  test("reject server invoices exceeding our limits") {
    withFixture { f =>
      f.server.loopOutSwapFee = amount => amount / 50
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat))).getMessage.contains("swap fee 20000 sat exceeds maximum 10000 sat"))
      f.server.loopOutSwapFee = amount => amount / 100
      f.server.loopOutPrepay = 50_000.sat
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, maxSwapFee_opt = Some(100_000 sat)))).getMessage.contains("prepay amount 50000 sat exceeds maximum"))
      assert(f.db.listSwaps().isEmpty)
      assert(f.spawned.isEmpty)
    }
  }

  test("log why a loop out couldn't be created") {
    LogCapture.withLogs(classOf[SwapManager]) { logs =>
      withFixture { f =>
        // Before we have a swap hash: we log the amount.
        awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, maxSwapFee_opt = Some(9999 sat))))
        // We have a swap hash, but we refuse the request before asking the server.
        awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, htlcConfirmations_opt = Some(0))))
        assert(f.server.loopOuts.isEmpty)
        // After the server created the swap: we log its hash.
        f.server.loopOutSwapFee = amount => amount / 50
        awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat)))
        val swapHash = f.server.loopOuts.keys.toSeq.loneElement
        f.server.loopOutSwapFee = amount => amount / 100
        // We throw instead of returning a failed future: we log the amount.
        val feerates = f.chain.feerates
        f.chain.feerates = _ => throw new RuntimeException("fee estimator unavailable")
        assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat))).getMessage == "fee estimator unavailable")
        f.chain.feerates = feerates
        val failures = logs().filter(_.getFormattedMessage.startsWith("cannot create loop out"))
        assert(failures.map(_.getLevel) == Seq(logback.Level.WARN, logback.Level.WARN, logback.Level.WARN, logback.Level.ERROR))
        val messages = failures.map(_.getFormattedMessage)
        assert(messages(0).startsWith("cannot create loop out amount=1000000 sat: swap fee 10000 sat exceeds maximum 9999 sat"))
        assert(messages(1).matches("cannot create loop out [0-9a-f]{64}: htlc confirmations must be at least 1"))
        assert(messages(2).startsWith(s"cannot create loop out $swapHash: swap fee 20000 sat exceeds maximum 10000 sat"))
        assert(messages(3) == "cannot create loop out amount=1000000 sat: fee estimator unavailable")
        // We released the key indexes of these swaps.
        val info = await(f.manager.loopOut(LoopOutRequest(1_000_000 sat)))
        assert(f.db.getSwap(info.swapHash).get.keyIndex == 0)
      }
    }
  }

  test("create a loop out once per idempotency key") {
    withFixture { f =>
      val request = LoopOutRequest(1_000_000 sat, idempotencyKey_opt = Some("payout-42"))
      val info = await(f.manager.loopOut(request))
      assert(info.idempotencyKey.contains("payout-42"))
      // The API call timed out and the user retries: we return the swap that the request created.
      assert(await(f.manager.loopOut(request)).swapHash == info.swapHash)
      // Also after a restart.
      val restarted = new SwapManager(f.env.server, f.db, config, randomKey().publicKey, f.chain, f.payments, f.env.keys, Block.RegtestGenesisBlock.hash, _ => ())
      assert(await(restarted.loopOut(request)).swapHash == info.swapHash)
      assert(f.server.loopOuts.size == 1 && f.db.listSwaps().size == 1 && f.spawned.size == 1)
      // The key can't be used for another request: we compare the parameters that the caller gave.
      Seq(request.copy(amount = 2_000_000 sat), request.copy(fast = true), request.copy(confTarget_opt = Some(config.defaultSweepConfTarget))).foreach { other =>
        assert(awaitFailure(f.manager.loopOut(other)).getMessage == "idempotency key payout-42 was already used for another request")
      }
      // Requests without a key, or with another key, create other swaps.
      await(f.manager.loopOut(request.copy(idempotencyKey_opt = None)))
      await(f.manager.loopOut(request.copy(idempotencyKey_opt = Some("payout-43"))))
      assert(f.server.loopOuts.size == 3 && f.db.listSwaps().size == 3)
    }
  }

  test("a retried loop out waits for the creation of its swap") {
    withFixture { f =>
      val serverReply = Promise[Unit]()
      f.server.newLoopOutSwapReply = serverReply.future
      val request = LoopOutRequest(1_000_000 sat, idempotencyKey_opt = Some("payout-42"))
      val first = f.manager.loopOut(request)
      // The API call timed out while we were creating its swap, and the user retries.
      val retry = f.manager.loopOut(request)
      // Another request can't use the key meanwhile.
      assert(awaitFailure(f.manager.loopOut(request.copy(amount = 2_000_000 sat))).getMessage == "idempotency key payout-42 was already used for another request")
      assert(!first.isCompleted && !retry.isCompleted)
      serverReply.success(())
      assert(await(retry).swapHash == await(first).swapHash)
      assert(f.server.loopOuts.size == 1 && f.db.listSwaps().size == 1 && f.spawned.size == 1)
    }
  }

  test("retry a loop out whose creation failed with the same idempotency key") {
    withFixture { f =>
      val request = LoopOutRequest(1_000_000 sat, idempotencyKey_opt = Some("payout-42"))
      // The server created the swap, but we couldn't persist it: it doesn't exist for us.
      f.db.failNext("addSwap")
      awaitFailure(f.manager.loopOut(request))
      val info = await(f.manager.loopOut(request))
      assert(f.db.listSwaps().map(_.swapHash) == Seq(info.swapHash))
      assert(f.server.loopOuts.size == 2 && f.spawned.size == 1)
      // We can't tell whether a request created a swap when our db is unavailable: we fail it.
      f.db.failNext("getSwapByIdempotencyKey")
      assert(awaitFailure(f.manager.loopOut(request)).getMessage.contains("db is unavailable"))
      assert(f.server.loopOuts.size == 2)
    }
  }

  test("refuse invalid idempotency keys") {
    withFixture { f =>
      Seq("" -> "must not be empty", "k" * 65 -> "at most 64 characters", "a\nb" -> "control characters").foreach { case (key, error) =>
        assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, idempotencyKey_opt = Some(key)))).getMessage.contains(error))
      }
      assert(f.server.loopOuts.isEmpty)
      assert(await(f.manager.loopOut(LoopOutRequest(1_000_000 sat, idempotencyKey_opt = Some("k" * 64)))).idempotencyKey.contains("k" * 64))
    }
  }

  test("only accept server quotes within our configured limits, unless the user sets limits") {
    // The server quotes a 1% swap fee and a 1337 sat prepay.
    withConfig(config.copy(maxSwapFeePpm = 5000)) { f =>
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat))).getMessage.contains("swap fee 10000 sat exceeds 5000 sat (loop.max-swap-fee-ppm=5000)"))
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, maxSwapFee_opt = Some(9999 sat)))).getMessage.contains("swap fee 10000 sat exceeds maximum 9999 sat"))
      assert(f.server.loopOuts.isEmpty)
      assert(await(f.manager.loopOut(LoopOutRequest(1_000_000 sat, maxSwapFee_opt = Some(10_000 sat)))).loopOut.get.limits.maxSwapFee == 10_000.sat)
    }
    withConfig(config.copy(maxPrepay = 1000 sat)) { f =>
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat))).getMessage.contains("prepay amount 1337 sat exceeds 1000 sat (loop.max-prepay-sat)"))
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, maxPrepayAmount_opt = Some(1336 sat)))).getMessage.contains("prepay amount 1337 sat exceeds maximum 1336 sat"))
      assert(f.server.loopOuts.isEmpty)
      assert(await(f.manager.loopOut(LoopOutRequest(1_000_000 sat, maxPrepayAmount_opt = Some(1337 sat)))).loopOut.get.limits.maxPrepayAmount == 1337.sat)
    }
  }

  test("refuse a max miner fee that can't pay for our sweep") {
    withFixture { f =>
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, maxMinerFee_opt = Some(100 sat)))).getMessage.contains("max miner fee 100 sat is below the estimated fee of our sweep"))
      assert(f.server.loopOuts.isEmpty)
      assert(f.db.listSwaps().isEmpty)
    }
  }

  test("pay the server through the first hop") {
    withFixture { f =>
      val firstHop = RealShortChannelId(BlockHeight(750_000), 1, 0)
      f.payments.channelBalances = Map(firstHop -> 2_000_000_000.msat, RealShortChannelId(BlockHeight(750_001), 2, 1) -> 5_000_000_000L.msat)
      val info = await(f.manager.loopOut(LoopOutRequest(1_000_000 sat, firstHop_opt = Some(firstHop))))
      assert(info.loopOut.get.firstHop.contains(firstHop))
      assert(f.swap().contract.firstHop_opt.contains(firstHop))
      assert(await(f.processor().step(f.swap())).state == States.Initiated)
      val prepayHash = Bolt11Invoice.fromString(f.swap().contract.prepayInvoice).get.paymentHash
      assert(f.payments.firstHops.toMap == Map(f.swap().swapHash -> Some(firstHop), prepayHash -> Some(firstHop)))
      // Without a first hop, eclair can use any of our channels.
      val info2 = await(f.manager.loopOut(LoopOutRequest(500_000 sat)))
      assert(info2.loopOut.get.firstHop.isEmpty)
      assert(await(f.processor().step(LoopOut.Swap(f.db.getSwap(info2.swapHash).get))).state == States.Initiated)
      assert(f.payments.firstHops(info2.swapHash).isEmpty)
      // eclair's router only knows an announced channel by its real scid: we use it if we're given its alias.
      f.payments.otherChannelIds = Map(Alias(42) -> firstHop)
      val info3 = await(f.manager.loopOut(LoopOutRequest(500_000 sat, firstHop_opt = Some(Alias(42)))))
      assert(info3.loopOut.get.firstHop.get.toString == "750000x1x0")
      assert(await(f.processor().step(LoopOut.Swap(f.db.getSwap(info3.swapHash).get))).state == States.Initiated)
      assert(f.payments.firstHops(info3.swapHash).contains(firstHop))
    }
  }

  test("refuse a first hop that can't pay the server") {
    withFixture { f =>
      val firstHop = RealShortChannelId(BlockHeight(750_000), 1, 0)
      // The swap and prepay invoices add up to the swap amount plus the swap fee (1%).
      f.payments.channelBalances = Map(firstHop -> 1_009_999_999.msat)
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, firstHop_opt = Some(Alias(42))))).getMessage.contains("first hop 0x2a isn't one of our open channels"))
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, firstHop_opt = Some(firstHop)))).getMessage.contains("our channel 750000x1x0 can only send 1009999999 msat now (its balance, within the value of htlcs in flight that our peer accepts), the swap needs 1010000000 msat"))
      // An offline channel can't send anything.
      f.payments.channelBalances = Map(firstHop -> 0.msat)
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, firstHop_opt = Some(firstHop)))).getMessage.contains("can only send 0 msat now"))
      assert(f.server.loopOuts.isEmpty)
      assert(f.db.listSwaps().isEmpty)
      f.payments.channelBalances = Map(firstHop -> 1_010_000_000.msat)
      assert(await(f.manager.loopOut(LoopOutRequest(1_000_000 sat, firstHop_opt = Some(firstHop)))).state == States.Initiated)
    }
  }

  test("wait for the first hop to be online before paying the server") {
    withFixture { f =>
      // An unannounced channel: eclair's router knows it by its alias.
      val firstHop = Alias(42)
      f.payments.channelBalances = Map(firstHop -> 2_000_000_000.msat)
      await(f.manager.loopOut(LoopOutRequest(1_000_000 sat, firstHop_opt = Some(firstHop))))
      assert(f.swap().contract.firstHop_opt.get.toString == "0x2a")
      // We restarted: our other channels are online, but not the first hop.
      f.payments.channelBalances = Map(firstHop -> 0.msat)
      assert(f.payments.canSendNow)
      val processor = f.processor()
      assert(await(processor.step(f.swap())).state == States.Initiated)
      assert(f.payments.paid.isEmpty)
      // It is back online, and was announced in the meantime: eclair's router now knows it by its real scid.
      val realScid = RealShortChannelId(BlockHeight(750_000), 1, 0)
      f.payments.channelBalances = Map(realScid -> 2_000_000_000.msat)
      f.payments.otherChannelIds = Map(firstHop -> realScid)
      assert(await(processor.step(f.swap())).state == States.Initiated)
      assert(f.payments.paid.size == 2)
      assert(f.payments.firstHops.values.toSet == Set(Some(realScid)))
    }
  }

  test("parse short channel ids like eclair displays them") {
    assert(SwapManager.parseShortChannelId("750000x1x0").get == RealShortChannelId(BlockHeight(750_000), 1, 0))
    assert(SwapManager.parseShortChannelId("0x1b865d25d5a34f4").get == Alias(0x1b865d25d5a34f4L))
    // Aliases are random 64-bit numbers, eclair displays them unsigned.
    assert(SwapManager.parseShortChannelId("0xf424000000000060").get.toLong == 0xf424000000000060L)
    assert(SwapManager.parseShortChannelId("0xf424000000000060").get.toString == "0xf424000000000060")
    assert(SwapManager.parseShortChannelId("750000x1").isFailure)
    assert(SwapManager.parseShortChannelId("0xzz").isFailure)
    assert(SwapManager.parseShortChannelId("").isFailure)
  }

  test("loop out with cooperative sweep") {
    withFixture { f =>
      val processor = startSwap(f)
      // We paid both invoices with our routing fee limits.
      assert(f.payments.paid.map(_._1.paymentHash).toSet == Set(f.swap().swapHash, Bolt11Invoice.fromString(f.swap().contract.prepayInvoice).get.paymentHash))
      assert(f.payments.paid.map(_._2).toSet == Set(20_010.sat, 36.sat))
      // We don't pay again when processing again (e.g. after a restart).
      assert(await(processor.step(f.swap())).state == States.Initiated)
      assert(await(f.processor().step(f.swap())).state == States.Initiated)
      assert(f.payments.paid.size == 2)
      // The htlc confirms: we reveal the preimage and sweep it with the server.
      f.chain.mineEmpty(3)
      val htlcTx = publishHtlc(f)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.PreimageRevealed)
      assert(swap1.progress.htlc_opt.get.outPoint.txid == htlcTx.txid)
      assert(f.server.pushedPreimages.get(swap1.swapHash).contains(swap1.record.preimage))
      assert(f.server.requestsOfType[MuSig2SignSweepReq].size == 1)
      val sweep = swap1.progress.sweepTxs.loneElement
      assert(sweep.txIn.head.witness.stack.size == 1) // key path
      assert(sweep.txOut.head.publicKeyScript == swap1.contract.sweepScript)
      assert(f.chain.mempoolTxs == Seq(sweep))
      // Nothing changes until the sweep confirms deeply enough.
      assert(await(processor.step(swap1)).progress.sweepTxs == Seq(sweep))
      f.chain.mine()
      assert(await(processor.step(f.swap())).state == States.PreimageRevealed)
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.Success)
      val info = f.manager.getSwap(swap1.swapHash).get
      assert(info.loopOut.get.minerFee.contains(fee(f, sweep)))
      assert(info.loopOut.get.sweepTxIds == Seq(sweep.txid))
    }
  }

  test("loop out with script path sweep when the server doesn't cooperate") {
    withFixture { f =>
      f.server.cooperativeSweeps = false
      val processor = startSwap(f)
      publishHtlc(f)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.PreimageRevealed)
      val sweep = swap1.progress.sweepTxs.loneElement
      assert(sweep.txIn.head.witness.stack.size == 4) // script path
      assert(f.htlc().extractPreimage(sweep.txIn.head.witness).contains(swap1.record.preimage))
      assert(swap1.progress.musig2Failures == 1)
      f.chain.mine()
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.Success)
    }
  }

  test("sweep through the script path when the server is unreachable") {
    withFixture { f =>
      val processor = startSwap(f)
      f.server.unavailable = true
      publishHtlc(f)
      // We can't push the preimage nor sweep cooperatively: we sweep alone.
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.PreimageRevealed)
      val sweep = swap1.progress.sweepTxs.loneElement
      assert(sweep.txIn.head.witness.stack.size == 4)
      assert(f.chain.mempoolTxs == Seq(sweep))
      f.chain.mine()
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.Success)
    }
  }

  test("stop trying cooperative sweeps after too many failures") {
    withFixture { f =>
      f.server.cooperativeSweeps = false
      val processor = startSwap(f)
      publishHtlc(f)
      await(processor.step(f.swap()))
      // We raise the feerate at every block to force a new sweep (a key path sweep is lighter than the script path
      // sweep it replaces, so the feerate must increase enough for it to pay a higher fee).
      (1 until LoopOut.MaxMusig2Failures + 3).foreach { i =>
        f.chain.feerates = _ => FeeratePerKw((2500 * math.pow(1.5, i)).toLong.sat)
        f.chain.mineEmpty(1)
        await(processor.step(f.swap()))
      }
      assert(f.swap().progress.musig2Failures == LoopOut.MaxMusig2Failures)
      assert(f.server.requestsOfType[MuSig2SignSweepReq].size == LoopOut.MaxMusig2Failures)
      assert(f.swap().progress.sweepTxs.forall(_.txIn.head.witness.stack.size == 4))
    }
  }

  test("bump the sweep fee when feerates increase or the htlc is about to expire") {
    withFixture { f =>
      f.chain.feerates = target => if (target <= LoopOut.UrgentSweepConfTarget) FeeratePerKw(10_000 sat) else FeeratePerKw(2500 sat)
      // We don't raise our feerate every block here.
      val processor = startSwap(f, feerateStep = FeeratePerKw(0 sat))
      publishHtlc(f)
      val sweep1 = await(processor.step(f.swap())).progress.sweepTxs.loneElement
      // Same feerate: we keep the same sweep (but re-broadcast it).
      f.chain.mineEmpty(1)
      assert(await(processor.step(f.swap())).progress.sweepTxs == Seq(sweep1))
      assert(f.chain.published.count(_.txid == sweep1.txid) == 2)
      // Higher feerate: we replace the sweep.
      f.chain.feerates = target => if (target <= LoopOut.UrgentSweepConfTarget) FeeratePerKw(10_000 sat) else FeeratePerKw(3000 sat)
      f.chain.mineEmpty(1)
      val sweep2 = await(processor.step(f.swap())).progress.sweepTxs.last
      assert(sweep2.txid != sweep1.txid)
      assert(fee(f, sweep2) > fee(f, sweep1))
      assert(f.chain.mempoolTxs == Seq(sweep2))
      // A feerate increase that doesn't change the fee doesn't replace the sweep.
      f.chain.feerates = target => if (target <= LoopOut.UrgentSweepConfTarget) FeeratePerKw(10_000 sat) else FeeratePerKw(3001 sat)
      f.chain.mineEmpty(1)
      assert(await(processor.step(f.swap())).progress.sweepTxs.last == sweep2)
      // A small increase still pays for the replacement's relay fee.
      f.chain.feerates = target => if (target <= LoopOut.UrgentSweepConfTarget) FeeratePerKw(10_000 sat) else FeeratePerKw(3100 sat)
      f.chain.mineEmpty(1)
      val sweep3 = await(processor.step(f.swap())).progress.sweepTxs.last
      assert(fee(f, sweep3) >= fee(f, sweep2) + fr.acinq.bitcoin.scalacompat.Satoshi(sweep3.weight() / 4))
      // Close to the expiry, we use the urgent confirmation target.
      f.chain.mineEmpty((f.swap().cltvExpiry - LoopOut.SweepConfTargetDelta - f.chain.height).toInt)
      val sweep4 = await(processor.step(f.swap())).progress.sweepTxs.last
      assert(fee(f, sweep4) >= fr.acinq.eclair.transactions.Transactions.weight2fee(FeeratePerKw(10_000 sat), sweep4.weight()))
      // An older sweep may confirm instead of the latest one.
      f.chain.mine(sweep2)
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.Success)
    }
  }

  test("raise the sweep feerate every block until it confirms") {
    withFixture { f =>
      val processor = startSwap(f)
      publishHtlc(f)
      val sweep1 = await(processor.step(f.swap())).progress.sweepTxs.loneElement
      val weight = sweep1.weight()
      val relayFee = Satoshi((weight + 3) / 4)
      assert(fee(f, sweep1) == Transactions.weight2fee(FeeratePerKw(2500 sat), weight))
      // Feerate estimates don't change, but our sweep doesn't confirm.
      (1 to 10).foreach { i =>
        f.chain.mineEmpty(1)
        val sweep = await(processor.step(f.swap())).progress.sweepTxs.last
        // We pay at least our raised feerate, and less than that plus what replacing our previous sweep requires.
        val targetFee = Transactions.weight2fee(FeeratePerKw(2500 sat) + LoopOut.SweepFeerateStep * i, weight)
        assert(targetFee <= fee(f, sweep) && fee(f, sweep) <= targetFee + relayFee)
      }
      // The step is smaller than the relay fee of a replacement: we don't replace our sweep in every block.
      val sweeps = f.swap().progress.sweepTxs
      assert(sweeps.size > 2 && sweeps.size < 11)
      // After a restart, we raise the feerate of our last sweep.
      f.chain.mineEmpty(1)
      assert(fee(f, await(f.processor().step(f.swap())).progress.sweepTxs.last) > fee(f, sweeps.last))
    }
  }

  test("never pay more than the max miner fee before the htlc is about to expire") {
    withFixture { f =>
      val processor = startSwap(f, LoopOutRequest(1_000_000 sat, maxMinerFee_opt = Some(2000 sat)))
      publishHtlc(f)
      val sweep1 = await(processor.step(f.swap())).progress.sweepTxs.loneElement
      assert(fee(f, sweep1) < 2000.sat)
      f.chain.feerates = _ => FeeratePerKw(100_000 sat)
      f.chain.mineEmpty(1)
      val sweep2 = await(processor.step(f.swap())).progress.sweepTxs.last
      assert(fee(f, sweep2) == 2000.sat)
      f.chain.mineEmpty(1)
      assert(await(processor.step(f.swap())).progress.sweepTxs.last == sweep2)
    }
  }

  test("pay more than the max miner fee when the htlc is about to expire") {
    withFixture { f =>
      val processor = startSwap(f, LoopOutRequest(1_000_000 sat, maxMinerFee_opt = Some(2000 sat)))
      publishHtlc(f)
      await(processor.step(f.swap()))
      f.chain.feerates = _ => FeeratePerKw(100_000 sat)
      f.chain.mineEmpty(1)
      assert(fee(f, await(processor.step(f.swap())).progress.sweepTxs.last) == 2000.sat)
      // We would lose the whole swap amount: we pay what our sweep needs...
      f.chain.mineEmpty((f.swap().cltvExpiry - LoopOut.SweepConfTargetDelta - f.chain.height).toInt)
      val sweep2 = await(processor.step(f.swap())).progress.sweepTxs.last
      assert(fee(f, sweep2) > 2000.sat)
      assert(fee(f, sweep2) >= fr.acinq.eclair.transactions.Transactions.weight2fee(FeeratePerKw(100_000 sat), sweep2.weight()))
      // ... up to a share of the htlc amount.
      f.chain.feerates = _ => FeeratePerKw(10_000_000 sat)
      f.chain.mineEmpty(1)
      assert(fee(f, await(processor.step(f.swap())).progress.sweepTxs.last) == LoopOut.maxFeeToAmount(1_000_000 sat))
    }
  }

  test("don't reveal the preimage while our sweep would exceed the max miner fee") {
    withFixture { f =>
      // Our sweep costs 1400 sat at this feerate.
      val processor = startSwap(f, LoopOutRequest(1_000_000 sat, maxMinerFee_opt = Some(2000 sat)))
      f.chain.feerates = _ => FeeratePerKw(5000 sat)
      publishHtlc(f)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.Initiated && swap1.progress.htlc_opt.nonEmpty)
      assert(f.server.pushedPreimages.isEmpty && swap1.progress.sweepTxs.isEmpty)
      // Feerates drop: we can sweep within our limits.
      f.chain.feerates = _ => FeeratePerKw(2500 sat)
      f.chain.mineEmpty(1)
      val swap2 = await(processor.step(f.swap()))
      assert(swap2.state == States.PreimageRevealed && f.server.pushedPreimages.contains(swap2.swapHash))
      assert(fee(f, swap2.progress.sweepTxs.loneElement) < 2000.sat)
    }
  }

  test("give up without revealing the preimage if our sweep exceeds the max miner fee until the reveal deadline") {
    withFixture { f =>
      val processor = startSwap(f, LoopOutRequest(1_000_000 sat, maxMinerFee_opt = Some(2000 sat)))
      f.chain.feerates = _ => FeeratePerKw(5000 sat)
      publishHtlc(f)
      assert(await(processor.step(f.swap())).state == States.Initiated)
      f.chain.mineEmpty((f.swap().cltvExpiry - LoopOut.MinPreimageRevealDelta - f.chain.height).toInt)
      assert(await(processor.step(f.swap())).state == States.FailedTimeout)
      assert(f.server.pushedPreimages.isEmpty && f.swap().progress.sweepTxs.isEmpty)
    }
  }

  test("fail if our payments fail before the htlc confirms") {
    withFixture { f =>
      val processor = startSwap(f)
      f.payments.statuses.put(f.swap().swapHash, SwapPaymentStatus.Failed)
      f.payments.failures.put(f.swap().swapHash, PaymentFailureInfo(PaymentFailureReason.LND_FAILURE_REASON_NO_ROUTE, Seq(1, 0xffffffffL)))
      assert(await(processor.step(f.swap())).state == States.FailedOffchainPayments)
      assert(f.server.requestsOfType[ServerLoopOutPushPreimageRequest].isEmpty)
      // We tell the server why we gave up, like loopd.
      val cancel = f.server.requestsOfType[CancelLoopOutSwapRequest].loneElement
      assert(ByteVector32(ByteVector(cancel.getSwapHash.toByteArray)) == f.swap().swapHash)
      assert(ByteVector(cancel.getPaymentAddress.toByteArray) == Bolt11Invoice.fromString(f.swap().contract.swapInvoice).get.paymentSecret.bytes)
      assert(cancel.getRouteCancel.getRouteType == RoutePaymentType.INVOICE_ROUTE)
      assert(cancel.getRouteCancel.getFailure == PaymentFailureReason.LND_FAILURE_REASON_NO_ROUTE)
      assert(cancel.getRouteCancel.getAttemptsList.asScala.map(a => Integer.toUnsignedLong(a.getRemainingHops)) == Seq(1, 0xffffffffL))
      // The server being unreachable doesn't prevent the swap from failing.
      withFixture { f2 =>
        val processor2 = startSwap(f2)
        f2.payments.statuses.put(Bolt11Invoice.fromString(f2.swap().contract.prepayInvoice).get.paymentHash, SwapPaymentStatus.Failed)
        f2.server.unavailable = true
        assert(await(processor2.step(f2.swap())).state == States.FailedOffchainPayments)
      }
    }
  }

  test("don't cancel a swap that the server already cancelled") {
    withFixture { f =>
      val processor = startSwap(f)
      // The server cancelled its hold invoices: the htlcs that reached it were failed with incorrect_or_unknown_payment_details.
      f.payments.statuses.put(f.swap().swapHash, SwapPaymentStatus.Failed)
      f.payments.failures.put(f.swap().swapHash, PaymentFailureInfo(PaymentFailureReason.LND_FAILURE_REASON_INCORRECT_PAYMENT_DETAILS, Seq(0)))
      assert(await(processor.step(f.swap())).state == States.FailedOffchainPayments)
      // The server only accepts one cancellation per swap, it would reject ours.
      assert(f.server.requestsOfType[CancelLoopOutSwapRequest].isEmpty)
    }
  }

  test("map eclair payment failures to loopd's failure reasons") {
    import fr.acinq.eclair.TimestampMilli
    import fr.acinq.eclair.db.{FailureSummary, FailureType, HopSummary, OutgoingPayment, OutgoingPaymentStatus}
    import fr.acinq.eclair.wire.protocol.IncorrectOrUnknownPaymentDetails
    val now = TimestampMilli.now()
    val (a, b, c, d) = (randomKey().publicKey, randomKey().publicKey, randomKey().publicKey, randomKey().publicKey)
    val route = List(HopSummary(a, b), HopSummary(b, c), HopSummary(c, d))

    def failed(failures: FailureSummary*): OutgoingPayment = OutgoingPayment(UUID.randomUUID(), UUID.randomUUID(), None, randomBytes32(), "Standard", MilliSatoshi(1000), MilliSatoshi(1000), d, now, None, None, OutgoingPaymentStatus.Failed(failures.toList, now))

    assert(EclairLoopPayments.failureInfo(Nil) == PaymentFailureInfo(PaymentFailureReason.LND_FAILURE_REASON_ERROR, Nil))
    assert(EclairLoopPayments.failureInfo(Seq(failed(FailureSummary(FailureType.LOCAL, "route not found", Nil, None)))).reason == PaymentFailureReason.LND_FAILURE_REASON_NO_ROUTE)
    assert(EclairLoopPayments.failureInfo(Seq(failed(FailureSummary(FailureType.LOCAL, "expiry too big: maximum=CltvExpiry(3212) actual=CltvExpiry(4529) blockHeight=1196", Nil, None)))).reason == PaymentFailureReason.LND_FAILURE_REASON_NO_ROUTE)
    assert(EclairLoopPayments.failureInfo(Seq(failed(FailureSummary(FailureType.LOCAL, "balance too low", Nil, None)))).reason == PaymentFailureReason.LND_FAILURE_REASON_INSUFFICIENT_BALANCE)
    val remote = EclairLoopPayments.failureInfo(Seq(failed(
      FailureSummary(FailureType.REMOTE, "temporary channel failure", route, Some(b)),
      // This is how eclair stores remote failures (FailureSummary.apply).
      FailureSummary(FailureType.REMOTE, IncorrectOrUnknownPaymentDetails(MilliSatoshi(1000), BlockHeight(100)).message, route, Some(d)),
    )))
    // b is 2 hops away from the destination, d is the destination.
    assert(remote == PaymentFailureInfo(PaymentFailureReason.LND_FAILURE_REASON_INCORRECT_PAYMENT_DETAILS, Seq(2, 0)))
    assert(EclairLoopPayments.failureInfo(Seq(failed(FailureSummary(FailureType.REMOTE, "fee insufficient", route, Some(a))))).remainingHops == Seq(0xffffffffL))
  }

  test("abandon a swap before revealing the preimage") {
    withFixture { f =>
      val processor = startSwap(f)
      val abandoned = processor.abandon(f.swap()).toOption.get
      assert(abandoned.state == States.FailedAbandoned)
      assert(f.swap().state == States.FailedAbandoned)
      assert(processor.isFinal(abandoned))
      assert(processor.abandon(f.swap()).left.toOption.get.contains("already final"))
    }
    withFixture { f =>
      val processor = startSwap(f)
      publishHtlc(f)
      assert(await(processor.step(f.swap())).state == States.PreimageRevealed)
      // From now on we must sweep.
      assert(processor.abandon(f.swap()).left.toOption.get.contains("preimage has been revealed"))
      assert(f.swap().state == States.PreimageRevealed)
    }
  }

  test("fail if the htlc doesn't confirm early enough") {
    withFixture { f =>
      val processor = startSwap(f)
      val revealDeadline = f.swap().cltvExpiry - LoopOut.MinPreimageRevealDelta
      f.chain.mineEmpty((revealDeadline - f.chain.height).toInt - 1)
      assert(await(processor.step(f.swap())).state == States.Initiated)
      // The htlc confirms too late to safely reveal the preimage.
      publishHtlc(f)
      assert(await(processor.step(f.swap())).state == States.FailedTimeout)
      assert(f.server.requestsOfType[ServerLoopOutPushPreimageRequest].isEmpty)
      assert(f.chain.published.isEmpty)
    }
  }

  test("fail if the htlc amount is too low") {
    withFixture { f =>
      val processor = startSwap(f)
      publishHtlc(f, 999_999 sat)
      assert(await(processor.step(f.swap())).state == States.FailedIncorrectHtlcAmount)
      assert(f.server.requestsOfType[ServerLoopOutPushPreimageRequest].isEmpty)
      assert(f.chain.published.isEmpty)
    }
  }

  test("fail if the server reclaims the htlc after we revealed the preimage") {
    withFixture { f =>
      val processor = startSwap(f)
      val htlcTx = publishHtlc(f)
      assert(await(processor.step(f.swap())).state == States.PreimageRevealed)
      assert(f.swap().progress.sweepTxs.nonEmpty)
      // Our sweep doesn't confirm (e.g. evicted from mempools), the server's timeout transaction does.
      f.chain.clearMempool()
      val serverSweep = Transaction(2, Seq(fr.acinq.bitcoin.scalacompat.TxIn(f.swap().progress.htlc_opt.get.outPoint, Nil, 0)), Seq(htlcTx.txOut(1).copy(amount = 990_000 sat)), 0)
      f.chain.mine(serverSweep)
      // A reorg may still undo it: we wait until it is deeply confirmed.
      assert(await(processor.step(f.swap())).state == States.PreimageRevealed)
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.FailedSweepTimeout)
    }
  }

  test("sweep again if the server's timeout transaction is reorganized out") {
    withFixture { f =>
      val processor = startSwap(f)
      val htlcTx = publishHtlc(f)
      assert(await(processor.step(f.swap())).state == States.PreimageRevealed)
      f.chain.clearMempool()
      val serverSweep = Transaction(2, Seq(TxIn(f.swap().progress.htlc_opt.get.outPoint, Nil, 0)), Seq(htlcTx.txOut(1).copy(amount = 990_000 sat)), 0)
      f.chain.mine(serverSweep)
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 2)
      assert(await(processor.step(f.swap())).state == States.PreimageRevealed)
      f.chain.disconnectTip()
      f.chain.disconnectTip()
      f.chain.mineEmpty(2)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.PreimageRevealed)
      assert(f.chain.mempoolTxs == Seq(swap1.progress.sweepTxs.last))
      f.chain.mine()
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.Success)
    }
  }

  test("recognize our sweeps that we didn't keep") {
    withFixture { f =>
      val processor = startSwap(f)
      publishHtlc(f)
      await(processor.step(f.swap()))
      // One of the sweeps that we published before keeping only the last ones confirms.
      f.chain.clearMempool()
      val htlcOutput = f.swap().progress.htlc_opt.get
      val oldSweep = Transaction(2, Seq(TxIn(htlcOutput.outPoint, Nil, 0)), Seq(TxOut(htlcOutput.txOut.amount - 1000.sat, f.swap().contract.sweepScript)), 0)
      f.chain.mine(oldSweep)
      assert(await(processor.step(f.swap())).state == States.PreimageRevealed)
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.Success)
    }
  }

  test("report the fee of the sweep that confirmed") {
    withFixture { f =>
      val processor = startSwap(f)
      publishHtlc(f)
      val sweep1 = await(processor.step(f.swap())).progress.sweepTxs.loneElement
      // We raise our feerate, but our first sweep confirms (our replacement didn't reach miners).
      f.chain.feerates = _ => FeeratePerKw(5000 sat)
      f.chain.mineEmpty(1)
      val sweep2 = await(processor.step(f.swap())).progress.sweepTxs.last
      assert(sweep2 != sweep1 && fee(f, sweep2) > fee(f, sweep1))
      f.chain.clearMempool()
      f.chain.mine(sweep1)
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.Success)
      val info = f.manager.getSwap(f.swap().swapHash).get.loopOut.get
      assert(info.minerFee.contains(fee(f, sweep1)))
      assert(info.sweepTxIds.last == sweep1.txid)
    }
    withFixture { f =>
      val processor = startSwap(f)
      publishHtlc(f)
      await(processor.step(f.swap()))
      // One of the sweeps that we published before keeping only the last ones confirms.
      f.chain.clearMempool()
      val htlcOutput = f.swap().progress.htlc_opt.get
      val oldSweep = Transaction(2, Seq(TxIn(htlcOutput.outPoint, Nil, 0)), Seq(TxOut(htlcOutput.txOut.amount - 1000.sat, f.swap().contract.sweepScript)), 0)
      f.chain.mine(oldSweep)
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.Success)
      val info = f.manager.getSwap(f.swap().swapHash).get.loopOut.get
      assert(info.minerFee.contains(1000.sat))
      assert(info.sweepTxIds.last == oldSweep.txid)
    }
  }

  test("push the preimage again until the server gets it") {
    withFixture { f =>
      val processor = startSwap(f)
      publishHtlc(f)
      // The server can't take our preimage now (it is down, or rejects our L402 token).
      f.server.pushPreimageFailure_opt = Some(Status.UNAVAILABLE)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.PreimageRevealed)
      assert(!swap1.progress.preimagePushed)
      // It doesn't co-sign without the preimage: we sweep through the script path.
      assert(swap1.progress.sweepTxs.loneElement.txIn.head.witness.stack.size == 4)
      // We push it again at the next step.
      f.server.pushPreimageFailure_opt = None
      f.chain.feerates = _ => FeeratePerKw(5000 sat)
      f.chain.mineEmpty(1)
      val swap2 = await(processor.step(f.swap()))
      assert(f.server.pushedPreimages.get(swap2.swapHash).contains(swap2.record.preimage))
      assert(swap2.progress.preimagePushed)
      // The server can now co-sign our sweep.
      assert(swap2.progress.sweepTxs.last.txIn.head.witness.stack.size == 1)
      // Once it has the preimage, we don't push it again.
      val pushes = f.server.requestsOfType[ServerLoopOutPushPreimageRequest].size
      f.chain.mineEmpty(1)
      await(processor.step(f.swap()))
      assert(f.server.requestsOfType[ServerLoopOutPushPreimageRequest].size == pushes)
    }
    withFixture { f =>
      val processor = startSwap(f)
      publishHtlc(f)
      f.server.pushPreimageFailure_opt = Some(Status.UNAVAILABLE)
      assert(await(processor.step(f.swap())).state == States.PreimageRevealed)
      val pushes = f.server.requestsOfType[ServerLoopOutPushPreimageRequest].size
      // The server learnt the preimage from our sweep and settled our payment: it doesn't need it anymore.
      f.payments.statuses.put(f.swap().swapHash, SwapPaymentStatus.Succeeded(f.swap().record.preimage, 1_000_000_000 msat, 1000 msat))
      f.chain.mineEmpty(1)
      await(processor.step(f.swap()))
      assert(f.server.requestsOfType[ServerLoopOutPushPreimageRequest].size == pushes)
    }
  }

  test("look for the htlc again after a reorg") {
    withFixture { f =>
      val processor = startSwap(f)
      f.server.cooperativeSweeps = false
      // The swap finds the htlc but doesn't reveal the preimage yet (we pretend it isn't confirmed anymore).
      val htlcTx = publishHtlc(f)
      f.chain.disconnectTip()
      f.chain.mineEmpty(1)
      assert(await(processor.step(f.swap())).progress.htlc_opt.isEmpty)
      // It is confirmed again in a later block.
      f.chain.mine(htlcTx)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.PreimageRevealed)
      assert(swap1.progress.htlc_opt.get.outPoint.txid == htlcTx.txid)
    }
  }

  test("look for the htlc in blocks that replaced blocks we already scanned") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mineEmpty(3)
      assert(await(processor.step(f.swap())).progress.scannedHeight == f.chain.height)
      // A reorg replaces the blocks we scanned: the htlc is in one of the new blocks, below the height we scanned.
      f.chain.disconnectTip()
      f.chain.disconnectTip()
      val htlcTx = publishHtlc(f)
      f.chain.mineEmpty(5)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.progress.htlc_opt.map(_.outPoint.txid).contains(htlcTx.txid))
      assert(swap1.state == States.PreimageRevealed)
    }
  }

  test("look for the htlc again when our best chain gets shorter than the blocks we scanned") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mineEmpty(3)
      await(processor.step(f.swap()))
      f.chain.disconnectTip()
      f.chain.disconnectTip()
      val htlcTx = publishHtlc(f)
      assert(f.chain.height < f.swap().progress.scannedHeight)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.progress.htlc_opt.map(_.outPoint.txid).contains(htlcTx.txid))
      assert(swap1.progress.scannedHeight == f.chain.height)
    }
  }

  test("sweep the htlc that replaced the htlc we revealed the preimage for") {
    withFixture { f =>
      val processor = startSwap(f)
      val htlcTx1 = publishHtlc(f)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.PreimageRevealed)
      val sweep1 = swap1.progress.sweepTxs.loneElement
      // The htlc is reorganized out, and the server double-spends it with another htlc for our swap.
      f.chain.disconnectTip()
      f.chain.clearMempool()
      val htlcTx2 = publishHtlc(f)
      // We keep the htlc we know until a block shows that the blocks we scanned were replaced.
      assert(await(processor.step(f.swap())).progress.htlc_opt.get.outPoint.txid == htlcTx1.txid)
      f.chain.mineEmpty(1)
      val swap2 = await(processor.step(f.swap()))
      assert(swap2.state == States.PreimageRevealed)
      assert(swap2.progress.htlc_opt.get.outPoint.txid == htlcTx2.txid)
      val sweep2 = swap2.progress.sweepTxs.last
      assert(sweep2.txIn.head.outPoint == swap2.progress.htlc_opt.get.outPoint)
      assert(swap2.progress.sweepTxs == Seq(sweep1, sweep2))
      f.chain.mine()
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.Success)
    }
  }

  test("wait for the htlc confirmations we asked for before revealing the preimage") {
    withFixture { f =>
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, htlcConfirmations_opt = Some(0)))).getMessage.contains("htlc confirmations must be at least 1"))
      // The htlc expires at 800103: we must reveal the preimage before 800083.
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, htlcConfirmations_opt = Some(83)))).getMessage.contains("htlc confirmations 83 leave no time to reveal the preimage before height 800083"))
      assert(f.server.loopOuts.isEmpty)
      val processor = startSwap(f, LoopOutRequest(1_000_000 sat, htlcConfirmations_opt = Some(3)))
      assert(f.manager.listSwaps().head.loopOut.get.htlcConfirmations == 3)
      publishHtlc(f)
      assert(await(processor.step(f.swap())).state == States.Initiated)
      f.chain.mineEmpty(1)
      assert(await(processor.step(f.swap())).state == States.Initiated)
      f.chain.mineEmpty(1)
      assert(await(processor.step(f.swap())).state == States.PreimageRevealed)
    }
  }

  test("don't pay our invoices again while eclair is sending them") {
    withFixture { f =>
      val processor = startSwap(f)
      assert(f.payments.paid.size == 2)
      // eclair only records our payments once it has found a route for them.
      f.payments.statuses.clear()
      assert(await(processor.step(f.swap())).state == States.Initiated)
      assert(f.payments.paid.size == 2)
    }
  }

  test("pay again if eclair rejected our payments before sending them") {
    withFixture { f =>
      await(f.manager.loopOut(LoopOutRequest(1_000_000 sat)))
      // eclair didn't answer in time: it may be sending our payments.
      f.payments.payFailure_opt = Some(new AskTimeoutException("timeout"))
      val processor1 = f.processor()
      awaitFailure(processor1.step(f.swap()))
      f.payments.payFailure_opt = None
      await(processor1.step(f.swap()))
      assert(f.payments.paid.isEmpty)
      // eclair rejected our payments before sending them.
      f.payments.payFailure_opt = Some(new IllegalArgumentException("invoice has expired"))
      val processor2 = f.processor()
      awaitFailure(processor2.step(f.swap()))
      f.payments.payFailure_opt = None
      awaitCond {
        await(processor2.step(f.swap()))
        f.payments.paid.size == 2
      }
      await(processor2.step(f.swap()))
      assert(f.payments.paid.size == 2)
    }
  }

  test("resume swaps after a restart") {
    withFixture { f =>
      val processor = startSwap(f)
      publishHtlc(f)
      val sweep = await(processor.step(f.swap())).progress.sweepTxs.loneElement
      // We restart: the swap is resumed from the db with its sweep.
      f.spawned = Nil
      f.manager.resumeSwaps()
      assert(f.spawned.map(_.swapHash) == Seq(f.swap().swapHash))
      assert(f.spawned.head.progress.sweepTxs == Seq(sweep))
      f.chain.mine()
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      assert(await(f.processor().step(f.spawned.head)).state == States.Success)
      assert(f.payments.paid.size == 2)
      // Final swaps aren't resumed.
      f.spawned = Nil
      f.manager.resumeSwaps()
      assert(f.spawned.isEmpty)
    }
  }

  test("pay again after a restart if eclair forgot our payments") {
    withFixture { f =>
      val processor = startSwap(f)
      val prepayHash = Bolt11Invoice.fromString(f.swap().contract.prepayInvoice).get.paymentHash
      assert(f.payments.paid.size == 2)
      // eclair stopped before committing the htlc of our swap payment: it will never complete.
      f.payments.orphaned = Set(f.swap().swapHash)
      f.spawned = Nil
      f.manager.resumeSwaps()
      assert(f.payments.orphanChecks == Seq(Set(f.swap().swapHash, prepayHash)))
      // Our channels haven't reconnected yet: the payment would fail right away, we wait.
      val processor1 = f.processor()
      f.payments.canSendNow = false
      assert(await(processor1.step(f.spawned.head)).state == States.Initiated)
      assert(f.payments.paid.size == 2)
      f.payments.canSendNow = true
      assert(await(processor1.step(f.spawned.head)).state == States.Initiated)
      assert(f.payments.paid.map(_._1.paymentHash) == Seq(f.swap().swapHash, prepayHash, f.swap().swapHash))
      // Final swaps aren't checked.
      f.payments.statuses.put(f.swap().swapHash, SwapPaymentStatus.Failed)
      assert(await(processor1.step(f.swap())).state == States.FailedOffchainPayments)
      f.manager.resumeSwaps()
      assert(f.payments.orphanChecks.last.isEmpty)
    }
  }

  test("pay again after a restart once the rest of an interrupted payment failed") {
    withFixture { f =>
      startSwap(f)
      val swapHash = f.swap().swapHash
      val prepayHash = Bolt11Invoice.fromString(f.swap().contract.prepayInvoice).get.paymentHash
      val firstAttempt = f.payments.paymentIds.head
      // eclair stopped after committing the htlcs of some parts of our swap payment, but not of others.
      f.payments.orphaned = Set(swapHash)
      f.spawned = Nil
      f.manager.resumeSwaps()
      f.payments.statuses.put(swapHash, SwapPaymentStatus.Pending)
      val processor = f.processor()
      assert(await(processor.step(f.spawned.head)).state == States.Initiated)
      assert(f.payments.paid.size == 2)
      // The server's node fails the parts it received once it gives up waiting for the others: we pay again.
      f.payments.statuses.remove(swapHash)
      assert(processor.onPaymentEvent(f.swap(), PaymentFailed(firstAttempt, swapHash, Nil, TimestampMilli.now())))
      assert(await(processor.step(f.swap())).state == States.Initiated)
      assert(f.payments.paid.map(_._1.paymentHash) == Seq(swapHash, prepayHash, swapHash))
      // The parts of our new payment failed, eclair may retry them: the failure of our first payment isn't theirs.
      f.payments.recentlyFailed = Set(swapHash)
      assert(await(processor.step(f.swap())).state == States.Initiated)
      assert(processor.onPaymentEvent(f.swap(), PaymentFailed(f.payments.paymentIds.last, swapHash, Nil, TimestampMilli.now())))
      assert(await(processor.step(f.swap())).state == States.FailedOffchainPayments)
    }
  }

  test("loop out expiry leaves a margin for our block height lagging behind the server's") {
    val terms = SwapServerClient.LoopOutTerms(250_000 sat, 1_000_000 sat, minCltvDelta = 100, maxCltvDelta = 400)
    val height = BlockHeight(1000)
    // Below the server's min delta: if we're one block behind the server, it would see a delta of 99 and refuse.
    assert(SwapManager.loopOutExpiry(height, terms, confTarget = 9) == Right(height + 100 + SwapManager.LoopOutExpiryMargin))
    assert(SwapManager.loopOutExpiry(height, terms, confTarget = 200) == Right(height + 200 + SwapManager.LoopOutExpiryMargin))
    assert(SwapManager.loopOutExpiry(height, terms, confTarget = 399) == Right(height + 400))
    assert(SwapManager.loopOutExpiry(height, terms, confTarget = 401).isLeft)
  }

  test("an htlc that wasn't signed when eclair stopped isn't committed") {
    import fr.acinq.eclair.CltvExpiry
    import fr.acinq.eclair.channel.{Origin, Upstream}
    import fr.acinq.eclair.wire.protocol.{OnionRoutingPacket, TlvStream, UpdateAddHtlc}
    val (signed, proposed) = (UUID.randomUUID(), UUID.randomUUID())
    val origins: Map[Long, Origin] = Map(
      129L -> Origin.Cold(Upstream.Local(signed)),
      130L -> Origin.Cold(Upstream.Local(proposed)),
      // An htlc that we relay isn't one of our payments.
      131L -> Origin.Cold(Upstream.Cold.Channel(randomBytes32(), randomKey().publicKey, 7, MilliSatoshi(1000))),
    )
    def add(id: Long): UpdateAddHtlc = UpdateAddHtlc(randomBytes32(), id, MilliSatoshi(30_000_000), randomBytes32(), CltvExpiry(1000), OnionRoutingPacket(0, ByteVector.fill(33)(2), ByteVector.fill(1300)(0), randomBytes32()), TlvStream.empty)
    // eclair stopped after adding htlc 130 but before signing it: it drops it when the channel reconnects, without
    // failing its payment (it only does so when disconnecting while running).
    assert(EclairLoopPayments.committedPaymentIds(origins, proposed = Seq(add(130))) == Set(signed))
    assert(EclairLoopPayments.committedPaymentIds(origins, proposed = Nil) == Set(signed, proposed))
  }

  test("detect payments that eclair forgot") {
    import fr.acinq.eclair.db.{OutgoingPayment, OutgoingPaymentStatus}
    import fr.acinq.eclair.TimestampMilli
    val now = TimestampMilli.now()
    val paymentHash = randomBytes32()
    val committed = OutgoingPayment(UUID.randomUUID(), UUID.randomUUID(), None, paymentHash, "Standard", MilliSatoshi(1000), MilliSatoshi(1000), randomKey().publicKey, now, None, None, OutgoingPaymentStatus.Pending)
    val forgotten = committed.copy(id = UUID.randomUUID())
    // A part of another payment for the same invoice.
    val failed = committed.copy(id = UUID.randomUUID(), parentId = UUID.randomUUID(), status = OutgoingPaymentStatus.Failed(Nil, now))
    assert(EclairLoopPayments.orphanedPayments(Seq(committed, forgotten, failed), committedPaymentIds = Set(committed.id)) == Seq(forgotten))
    // Once failed, forgotten payments are ignored: the invoice can be paid again.
    val orphaned = forgotten.copy(status = OutgoingPaymentStatus.Failed(Seq(EclairLoopPayments.orphanedFailureSummary), now))
    assert(EclairLoopPayments.status(Seq(orphaned), failureFinal = true, now) == SwapPaymentStatus.NotSent)
    assert(EclairLoopPayments.status(Seq(orphaned, committed), failureFinal = true, now) == SwapPaymentStatus.Pending)
    assert(EclairLoopPayments.status(Seq(orphaned, failed), failureFinal = true, now) == SwapPaymentStatus.Failed)
    assert(EclairLoopPayments.failureInfo(Seq(orphaned)) == PaymentFailureInfo(PaymentFailureReason.LND_FAILURE_REASON_ERROR, Nil))
  }

  test("a payment that eclair partly forgot can be paid again once its other parts failed") {
    import fr.acinq.eclair.db.{FailureSummary, FailureType, OutgoingPayment, OutgoingPaymentStatus}
    val now = TimestampMilli.now()
    val paymentHash = randomBytes32()
    val committed = OutgoingPayment(UUID.randomUUID(), UUID.randomUUID(), None, paymentHash, "Standard", MilliSatoshi(1000), MilliSatoshi(2000), randomKey().publicKey, now, None, None, OutgoingPaymentStatus.Pending)
    val orphaned = committed.copy(id = UUID.randomUUID(), status = OutgoingPaymentStatus.Failed(Seq(EclairLoopPayments.orphanedFailureSummary), now))
    // The recipient waits for the rest of the payment, which will never come...
    assert(EclairLoopPayments.status(Seq(committed, orphaned), failureFinal = true, now) == SwapPaymentStatus.Pending)
    // ...and fails the parts that it received after a while: the invoice can be paid again.
    val timedOut = committed.copy(status = OutgoingPaymentStatus.Failed(Seq(FailureSummary(FailureType.REMOTE, "mpp timeout", Nil, None)), now))
    assert(EclairLoopPayments.status(Seq(timedOut, orphaned), failureFinal = true, now) == SwapPaymentStatus.NotSent)
    assert(EclairLoopPayments.failureInfo(Seq(timedOut, orphaned)) == PaymentFailureInfo(PaymentFailureReason.LND_FAILURE_REASON_ERROR, Nil))
    // Payments that we send again count as usual.
    val retry = committed.copy(id = UUID.randomUUID(), parentId = UUID.randomUUID(), status = OutgoingPaymentStatus.Failed(Seq(FailureSummary(FailureType.LOCAL, "route not found", Nil, None)), now))
    assert(EclairLoopPayments.status(Seq(timedOut, orphaned, retry), failureFinal = true, now) == SwapPaymentStatus.Failed)
    assert(EclairLoopPayments.failureInfo(Seq(timedOut, orphaned, retry)).reason == PaymentFailureReason.LND_FAILURE_REASON_NO_ROUTE)
    // A part that the recipient settled gives us the preimage.
    val settled = committed.copy(status = OutgoingPaymentStatus.Succeeded(ByteVector32.One, MilliSatoshi(5), Nil, now))
    assert(EclairLoopPayments.status(Seq(settled, orphaned), failureFinal = true, now) == SwapPaymentStatus.Succeeded(ByteVector32.One, MilliSatoshi(1000), MilliSatoshi(5)))
  }

  test("payment status from eclair's payments db") {
    import fr.acinq.eclair.db.{OutgoingPayment, OutgoingPaymentStatus}
    import fr.acinq.eclair.TimestampMilli
    val now = TimestampMilli.now()
    val paymentHash = randomBytes32()
    val part = OutgoingPayment(UUID.randomUUID(), UUID.randomUUID(), None, paymentHash, "Standard", MilliSatoshi(1000), MilliSatoshi(1000), randomKey().publicKey, now, None, None, OutgoingPaymentStatus.Pending)
    val failed = part.copy(id = UUID.randomUUID(), status = OutgoingPaymentStatus.Failed(Nil, now))
    val oldFailure = part.copy(id = UUID.randomUUID(), status = OutgoingPaymentStatus.Failed(Nil, now - 10.minutes))
    val succeeded = part.copy(id = UUID.randomUUID(), status = OutgoingPaymentStatus.Succeeded(ByteVector32.One, MilliSatoshi(5), Nil, now))
    assert(EclairLoopPayments.status(Nil, failureFinal = false) == SwapPaymentStatus.NotSent)
    assert(EclairLoopPayments.status(Seq(part, failed), failureFinal = false) == SwapPaymentStatus.Pending)
    assert(EclairLoopPayments.status(Seq(failed, succeeded), failureFinal = false) == SwapPaymentStatus.Succeeded(ByteVector32.One, MilliSatoshi(1000), MilliSatoshi(5)))
    // eclair may still be retrying a payment whose parts just failed.
    assert(EclairLoopPayments.status(Seq(failed), failureFinal = false, now) == SwapPaymentStatus.Pending)
    assert(EclairLoopPayments.status(Seq(failed), failureFinal = true, now) == SwapPaymentStatus.Failed)
    assert(EclairLoopPayments.status(Seq(oldFailure), failureFinal = false, now) == SwapPaymentStatus.Failed)
  }

  test("encode and decode loop out data") {
    withFixture { f =>
      val processor = startSwap(f)
      publishHtlc(f)
      val swap = await(processor.step(f.swap()))
      assert(swap.progress.htlc_opt.nonEmpty && swap.progress.sweepTxs.nonEmpty)
      val data = swap.data.copy(contract = swap.contract.copy(htlcConfirmations = 3))
      assert(data.progress.scannedBlockId_opt.nonEmpty)
      assert(LoopOutData.decode(LoopOutData.encode(data)) == data)
      assert(LoopOutData.encode(data).head == 2)
      // We display first hops like eclair: aliases in hex, other short channel ids as coordinates.
      Seq(Alias(0x1b865d25d5a34f4L), RealShortChannelId(BlockHeight(750_000), 1, 0), ShortChannelId(42)).foreach { firstHop =>
        val data1 = data.copy(contract = data.contract.copy(firstHop_opt = Some(firstHop)))
        val decoded = LoopOutData.decode(LoopOutData.encode(data1))
        assert(decoded == data1)
        assert(decoded.contract.firstHop_opt.get.toString == firstHop.toString)
      }
      // Swaps created before we persisted their first hop were paid through any of our channels.
      val v1 = ByteVector(1) ++ LoopOutData.dataCodecV1.encode(data).require.bytes
      assert(LoopOutData.decode(v1) == data)
      // Swaps created before we persisted their htlc confirmations and scanned block used 1 confirmation.
      val v0 = ByteVector(0) ++ LoopOutData.dataCodecV0.encode(data).require.bytes
      assert(LoopOutData.decode(v0) == data.copy(contract = data.contract.copy(htlcConfirmations = 1), progress = data.progress.copy(scannedBlockId_opt = None)))
    }
  }

  private def awaitCond(p: => Boolean): Unit = {
    val deadline = System.currentTimeMillis() + 10_000
    while (!p) {
      if (System.currentTimeMillis() > deadline) fail("condition not met")
      Thread.sleep(10)
    }
  }

  implicit class LoneElement[T](s: Seq[T]) {
    def loneElement: T = {
      assert(s.size == 1, s"expected one element, got $s")
      s.head
    }
  }

}
