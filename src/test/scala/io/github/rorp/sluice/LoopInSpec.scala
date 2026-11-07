package io.github.rorp.sluice

import ch.qos.logback.{classic => logback}
import fr.acinq.bitcoin.ScriptFlags
import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.bitcoin.scalacompat.{Block, ByteVector32, Crypto, Musig2, SatoshiLong, Script, Transaction, TxOut}
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import io.github.rorp.sluice.LoopIn.States
import io.github.rorp.sluice.SwapManager.{LoopInRequest, LoopOutRequest}
import io.github.rorp.sluice.db.{LoopDb, SqliteLoopDb, SwapType}
import io.github.rorp.sluice.swapserverrpc.{RouteHint, ServerLoopInQuoteRequest, ServerLoopInRequest, ServerProbeRequest, ServerPushKeyReq}
import fr.acinq.eclair.payment.Bolt11Invoice.ExtraHop
import fr.acinq.eclair.payment.{PaymentEvent, PaymentReceived}
import fr.acinq.eclair.{BlockHeight, CltvExpiryDelta, MilliSatoshi, MilliSatoshiLong, ShortChannelId, TimestampMilli, randomBytes32, randomKey}
import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.funsuite.AnyFunSuite
import scodec.bits.ByteVector

import java.sql.DriverManager
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.ListHasAsScala

class LoopInSpec extends AnyFunSuite {

  import org.scalatest.LoneElement._

  implicit val ec: ExecutionContext = ExecutionContext.global

  private val config = SluiceConfig("in-process", ServerTls.Insecure, "sluice/v0.1.0-test", 10 seconds, maxL402Cost = 1000 sat, maxL402RoutingFee = 10 sat, l402PaymentTimeout = 10 seconds, l402MinPurchaseInterval = 1 hour, defaultSweepConfTarget = 9, loopOutHtlcConfirmations = 1, maxSwapFeePpm = 20_000, maxPrepay = 30_000 sat)

  case class Fixture(server: FakeSwapServer, chain: FakeLoopChain, payments: FakeLoopPayments, db: FlakyLoopDb, manager: SwapManager, env: SwapEnv, keys: LoopKeys) {
    @volatile var spawned = Seq.empty[LoopIn.Swap]

    def processor(): LoopInProcessor = new LoopInProcessor(env)

    def swap(): LoopIn.Swap = LoopIn.Swap(db.getSwap(spawned.head.swapHash).get)

    def htlc(): Htlc = LoopIn.htlc(swap(), keys)

    def htlcOutput(): HtlcOutput = LoopIn.htlcOutput(swap(), keys).get

    def serverSwap(): server.FakeLoopIn = server.loopIns(swap().swapHash)
  }

  def withFixture(test: Fixture => Unit): Unit = withConfig(config)(test)

  def withConfig(config: SluiceConfig)(test: Fixture => Unit): Unit = {
    val server = new FakeSwapServer()
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
      case swap: LoopIn.Swap => fixture.spawned = fixture.spawned :+ swap
      case _ => ()
    })
    fixture = Fixture(server, chain, payments, db, manager, env, keys)
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

  /** Creates a swap, runs the first step (publishing our htlc) and returns the processor. */
  private def startSwap(f: Fixture, request: LoopInRequest = LoopInRequest(1_000_000 sat)): LoopInProcessor = {
    val info = await(f.manager.loopIn(request))
    assert(info.state == States.Initiated)
    assert(f.spawned.map(_.swapHash) == Seq(info.swapHash))
    val processor = f.processor()
    assert(await(processor.step(f.swap())).state == States.HtlcPublished)
    processor
  }

  /** The server pays our invoice: returns false if it can't. */
  private def serverPays(f: Fixture): Boolean = f.payments.receive(f.swap().swapHash)

  /** The server sweeps the htlc with the preimage it learnt by paying us. */
  private def serverSuccessSweep(f: Fixture): Transaction = {
    val htlcOutput = f.htlcOutput()
    val unsigned = Htlc.sweepTx(htlcOutput.outPoint, htlcOutput.txOut.amount, Script.write(Script.pay2wpkh(randomKey().publicKey)), 1000 sat, keyPath = false)
    Htlc.signSuccess(f.htlc(), unsigned, htlcOutput.txOut, f.serverSwap().htlcKey, f.swap().record.preimage)
  }

  test("create loop in swap") {
    withFixture { f =>
      val info = await(f.manager.loopIn(LoopInRequest(1_000_000 sat)))
      assert(info.swapType == SwapType.LoopIn.name)
      assert(info.amount == 1_000_000.sat)
      assert(info.cltvExpiry == BlockHeight(801_000))
      val details = info.loopIn.get
      assert(details.swapFee == 5000.sat)
      assert(details.htlcConfTarget == LoopIn.DefaultHtlcConfTarget)
      assert(details.limits == LoopInLimits(maxSwapFee = 5000 sat, maxMinerFee = 4575 sat))
      assert(details.htlcOutPoint.isEmpty && details.amountReceived.isEmpty)
      val record = f.db.getSwap(info.swapHash).get
      assert(Crypto.sha256(record.preimage) == info.swapHash)
      // Our invoice is for the swap amount minus the swap fee.
      val (swapInvoice, status) = f.payments.invoices(info.swapHash)
      assert(swapInvoice.amount_opt.contains(MilliSatoshi(995_000_000)))
      assert(status == IncomingInvoiceStatus.Pending)
      // The server got our keys and invoices; the probe invoice can't be paid.
      val request = f.server.requestsOfType[ServerLoopInRequest].last
      assert(request.getSenderKey.toByteArray.toSeq == f.keys.swapKey(0).publicKey.value.toArray.toSeq)
      assert(request.getSenderInternalPubkey.toByteArray.toSeq == f.keys.internalKey(0, info.swapHash).publicKey.value.toArray.toSeq)
      assert(f.keys.internalKey(0, info.swapHash) != f.keys.swapKey(0))
      val serverSwap = f.server.loopIns(info.swapHash)
      assert(serverSwap.swapInvoice == swapInvoice)
      assert(serverSwap.probeInvoice.paymentHash == SwapManager.probeHash(info.swapHash))
      assert(serverSwap.probeInvoice.paymentHash != info.swapHash)
      assert(serverSwap.probeInvoice.amount_opt == swapInvoice.amount_opt)
      assert(f.payments.incomingStatus(serverSwap.probeInvoice.paymentHash) == IncomingInvoiceStatus.Unknown)
      // We build the same htlc as the server.
      assert(serverSwap.htlc == f.htlc())
      assert(details.htlcAddress == f.htlc().address(Block.RegtestGenesisBlock.hash))
    }
  }

  test("probe hash is the swap hash's hash with its first bit flipped (like loopd)") {
    val swapHash = randomBytes32()
    val h = Crypto.sha256(swapHash)
    val probeHash = SwapManager.probeHash(swapHash)
    assert(probeHash.bytes.head == (h.bytes.head ^ 1).toByte)
    assert(probeHash.bytes.tail == h.bytes.tail)
  }

  test("only accept server quotes within our configured limits, unless the user sets limits") {
    // The server quotes a 0.5% swap fee.
    withConfig(config.copy(maxSwapFeePpm = 4000)) { f =>
      assert(awaitFailure(f.manager.loopIn(LoopInRequest(1_000_000 sat))).getMessage.contains("swap fee 5000 sat exceeds 4000 sat (loop.max-swap-fee-ppm=4000)"))
      assert(f.server.loopIns.isEmpty && f.payments.invoices.isEmpty)
      assert(await(f.manager.loopIn(LoopInRequest(1_000_000 sat, maxSwapFee_opt = Some(5000 sat)))).loopIn.get.swapFee == 5000.sat)
    }
  }

  test("reject swaps exceeding our limits") {
    withFixture { f =>
      assert(awaitFailure(f.manager.loopIn(LoopInRequest(1_000_000 sat, maxSwapFee_opt = Some(4000 sat)))).getMessage.contains("swap fee 5000 sat exceeds maximum 4000 sat"))
      assert(awaitFailure(f.manager.loopIn(LoopInRequest(1_000_000 sat, htlcConfTarget_opt = Some(1)))).getMessage.contains("confirmation target"))
      // The server's htlc would lock our funds for too long, or not long enough: we delete our invoice.
      f.server.loopInExpiry = BlockHeight(800_000 + LoopIn.MaxLoopInAcceptDelta + 1)
      assert(awaitFailure(f.manager.loopIn(LoopInRequest(1_000_000 sat))).getMessage.contains("too far"))
      f.server.loopInExpiry = BlockHeight(800_000 + LoopIn.MinLoopInExpiryDelta - 1)
      assert(awaitFailure(f.manager.loopIn(LoopInRequest(1_000_000 sat))).getMessage.contains("too soon"))
      assert(f.server.loopIns.size == 2)
      assert(f.payments.invoices.isEmpty)
      assert(f.db.listSwaps().isEmpty)
      assert(f.spawned.isEmpty)
    }
  }

  test("log why a loop in couldn't be created") {
    LogCapture.withLogs(classOf[SwapManager]) { logs =>
      withFixture { f =>
        // Before we have a swap hash: we log the amount.
        awaitFailure(f.manager.loopIn(LoopInRequest(1_000_000 sat, maxSwapFee_opt = Some(4000 sat))))
        // After the server created the swap: we log its hash.
        val loopInExpiry = f.server.loopInExpiry
        f.server.loopInExpiry = BlockHeight(800_000 + LoopIn.MaxLoopInAcceptDelta + 1)
        awaitFailure(f.manager.loopIn(LoopInRequest(1_000_000 sat)))
        val swapHash = f.server.loopIns.keys.loneElement
        f.server.loopInExpiry = loopInExpiry
        // We throw instead of returning a failed future: we log the amount.
        f.payments.invoiceFailure_opt = Some(new RuntimeException("cannot create invoice"))
        assert(awaitFailure(f.manager.loopIn(LoopInRequest(1_000_000 sat))).getMessage == "cannot create invoice")
        f.payments.invoiceFailure_opt = None
        val failures = logs().filter(_.getFormattedMessage.startsWith("cannot create loop in"))
        assert(failures.map(_.getLevel) == Seq(logback.Level.WARN, logback.Level.WARN, logback.Level.ERROR))
        val messages = failures.map(_.getFormattedMessage)
        assert(messages(0).startsWith("cannot create loop in amount=1000000 sat: swap fee 5000 sat exceeds maximum 4000 sat"))
        assert(messages(1).startsWith(s"cannot create loop in $swapHash: "))
        assert(messages(1).contains("too far"))
        assert(messages(2) == "cannot create loop in amount=1000000 sat: cannot create invoice")
        // We released the key indexes of these swaps.
        val info = await(f.manager.loopIn(LoopInRequest(1_000_000 sat)))
        assert(f.db.getSwap(info.swapHash).get.keyIndex == 0)
      }
    }
  }

  test("loop in where the server sweeps through the key path") {
    withFixture { f =>
      val processor = startSwap(f)
      // We published our htlc.
      val htlcTx = f.chain.mempoolTxs.loneElement
      assert(f.swap().progress.htlcTx_opt.contains(htlcTx))
      assert(f.swap().progress.htlcTxFee == 1525.sat)
      assert(f.htlcOutput().txOut.amount == 1_000_000.sat)
      assert(await(processor.step(f.swap())).state == States.HtlcPublished)
      f.chain.mine()
      assert(await(processor.step(f.swap())).state == States.HtlcPublished)
      // The server pays us: we give it our internal key.
      assert(serverPays(f))
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.InvoiceSettled)
      assert(swap1.progress.keyPushed)
      assert(f.server.pushedKeys(swap1.swapHash) == f.keys.internalKey(swap1.record.keyIndex, swap1.swapHash))
      assert(f.server.requestsOfType[ServerPushKeyReq].size == 1)
      // The server can sweep the htlc through the key path on its own.
      val htlc = f.htlc()
      val htlcOutput = f.htlcOutput()
      val (clientKey, serverKey) = (f.server.pushedKeys(swap1.swapHash), f.serverSwap().internalKey)
      val tx = Htlc.sweepTx(htlcOutput.outPoint, htlcOutput.txOut.amount, Script.write(Script.pay2wpkh(randomKey().publicKey)), 1000 sat, keyPath = true)
      val nonces = Seq(clientKey, serverKey).map(k => Musig2.generateNonce(randomBytes32(), Left(k), htlc.internalKeys, None, None))
      val partialSigs = Seq(clientKey, serverKey).zip(nonces).map { case (k, n) => Musig2.signTaprootInput(k, tx, 0, Seq(htlcOutput.txOut), htlc.internalKeys, n.secretNonce, nonces.map(_.publicNonce), Some(htlc.scriptTree)).toOption.get }
      val sig = Musig2.aggregateTaprootSignatures(partialSigs, tx, 0, Seq(htlcOutput.txOut), htlc.internalKeys, nonces.map(_.publicNonce), Some(htlc.scriptTree)).toOption.get
      val sweep = tx.updateWitness(0, Script.witnessKeyPathPay2tr(sig))
      Transaction.correctlySpends(sweep, Map(htlcOutput.outPoint -> htlcOutput.txOut), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
      assert(await(processor.step(f.swap())).state == States.InvoiceSettled)
      f.chain.mine(sweep)
      assert(await(processor.step(f.swap())).state == States.Success)
      val info = f.manager.getSwap(swap1.swapHash).get.loopIn.get
      assert(info.amountReceived.contains(MilliSatoshi(995_000_000)))
      assert(info.htlcTxFee.contains(1525.sat))
      assert(info.htlcOutPoint.contains(s"${htlcTx.txid}:1"))
    }
  }

  test("loop in where the server sweeps with the preimage") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      assert(serverPays(f))
      f.server.pushedKeys.clear()
      f.chain.mine(serverSuccessSweep(f))
      assert(await(processor.step(f.swap())).state == States.Success)
    }
  }

  test("complete the swap once paid even if the server doesn't sweep") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      assert(serverPays(f))
      assert(await(processor.step(f.swap())).state == States.InvoiceSettled)
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt - 1)
      assert(await(processor.step(f.swap())).state == States.InvoiceSettled)
      f.chain.mineEmpty(1)
      // We were paid: we never refund.
      assert(await(processor.step(f.swap())).state == States.Success)
      assert(f.swap().progress.refundTxs.isEmpty)
    }
  }

  test("refund the htlc if the server doesn't pay us") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt - 1)
      assert(await(processor.step(f.swap())).progress.refundTxs.isEmpty)
      f.chain.mineEmpty(1)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.HtlcPublished)
      // We deleted our invoice before refunding: the server can't pay it anymore.
      assert(f.payments.incomingStatus(swap1.swapHash) == IncomingInvoiceStatus.Unknown)
      assert(!serverPays(f))
      val refund = f.chain.mempoolTxs.loneElement
      assert(swap1.progress.refundTxs == Seq(refund))
      assert(refund.lockTime == swap1.cltvExpiry.toLong)
      assert(refund.txOut.loneElement.publicKeyScript == swap1.contract.refundScript)
      assert(f.htlc().isTimeoutWitness(refund.txIn.head.witness))
      val htlcOutput = f.htlcOutput()
      Transaction.correctlySpends(refund, Map(htlcOutput.outPoint -> htlcOutput.txOut), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
      assert(refund.weight() == f.htlc().refundWeight(swap1.contract.refundScript))
      // Nothing changes in the same block.
      assert(await(processor.step(f.swap())).progress.refundTxs == Seq(refund))
      // Feerates rise: we bump the refund fee in the next block.
      f.chain.feerates = _ => FeeratePerKw(5000 sat)
      f.chain.clearMempool()
      f.chain.mineEmpty(1)
      val swap2 = await(processor.step(f.swap()))
      assert(swap2.progress.refundTxs.size == 2)
      val refund2 = swap2.progress.refundTxs.last
      assert(refund2.txOut.head.amount < refund.txOut.head.amount)
      assert(f.chain.mempoolTxs == Seq(refund2))
      f.chain.mine()
      assert(await(processor.step(f.swap())).state == States.HtlcPublished)
      f.chain.mineEmpty(LoopIn.RefundMinDepth - 1)
      val swap3 = await(processor.step(f.swap()))
      assert(swap3.state == States.FailedTimeout)
      assert(f.manager.getSwap(swap3.swapHash).get.loopIn.get.refundFee.contains(1_000_000.sat - refund2.txOut.head.amount))
    }
  }

  test("don't refund if the server pays us at the last moment") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt)
      assert(serverPays(f))
      assert(await(processor.step(f.swap())).state == States.Success)
      assert(f.swap().progress.refundTxs.isEmpty)
      assert(f.payments.incomingStatus(f.swap().swapHash).isInstanceOf[IncomingInvoiceStatus.Received])
    }
  }

  test("wait for our payment if the server swept the htlc after its expiry") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt)
      // The payment isn't recorded yet (eclair is still fulfilling it), but the server already used the preimage.
      f.chain.mine(serverSuccessSweep(f))
      assert(await(processor.step(f.swap())).state == States.HtlcPublished)
      assert(f.swap().progress.refundTxs.isEmpty)
      assert(serverPays(f))
      assert(await(processor.step(f.swap())).state == States.Success)
    }
  }

  test("fail if it's too late to publish the htlc") {
    withFixture { f =>
      await(f.manager.loopIn(LoopInRequest(1_000_000 sat)))
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt - LoopIn.MinLoopInPublishDelta + 1)
      val swap1 = await(f.processor().step(f.swap()))
      assert(swap1.state == States.FailedTimeout)
      assert(swap1.progress.htlcTx_opt.isEmpty)
      assert(f.chain.mempoolTxs.isEmpty)
      assert(f.payments.invoices.isEmpty)
    }
  }

  test("fail if we can't fund the htlc") {
    withFixture { f =>
      await(f.manager.loopIn(LoopInRequest(1_000_000 sat)))
      f.chain.walletEmpty = true
      assert(await(f.processor().step(f.swap())).state == States.FailedInsufficientFunds)
      assert(f.payments.invoices.isEmpty)
      // The fee of the htlc transaction is capped.
      f.chain.walletEmpty = false
      f.spawned = Nil
      await(f.manager.loopIn(LoopInRequest(1_000_000 sat, maxMinerFee_opt = Some(1000 sat))))
      assert(await(f.processor().step(f.swap())).state == States.FailedInsufficientFunds)
    }
  }

  test("give up on an htlc that can't be published anymore") {
    withFixture { f =>
      val processor = startSwap(f)
      val htlcTx = f.swap().progress.htlcTx_opt.get
      f.chain.clearMempool()
      // Its inputs have been spent by another transaction.
      f.chain.mine(Transaction(2, htlcTx.txIn, Seq(TxOut(10_000 sat, Script.write(Script.pay2wpkh(randomKey().publicKey)))), 0))
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt - 1)
      assert(await(processor.step(f.swap())).state == States.HtlcPublished)
      f.chain.mineEmpty(1)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.FailedTimeout)
      assert(f.chain.rolledBack == Seq(htlcTx))
      assert(f.payments.invoices.isEmpty)
    }
  }

  test("refund our htlc if it confirms after its expiry") {
    withFixture { f =>
      val processor = startSwap(f)
      val htlcTx = f.swap().progress.htlcTx_opt.get
      // Our htlc is evicted from our mempool, which then rejects it until it expires.
      f.chain.clearMempool()
      f.chain.rejected = _.txid == htlcTx.txid
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt)
      // It may still confirm (bitcoind's wallet and other nodes may broadcast it again): we don't give up on it.
      assert(await(processor.step(f.swap())).state == States.HtlcPublished)
      assert(f.chain.rolledBack.isEmpty)
      f.chain.rejected = _ => false
      f.chain.mine(htlcTx)
      val refund = await(processor.step(f.swap())).progress.refundTxs.loneElement
      assert(refund.txIn.head.outPoint == f.htlcOutput().outPoint)
      assert(f.payments.invoices.isEmpty)
      f.chain.mine()
      f.chain.mineEmpty(LoopIn.RefundMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.FailedTimeout)
    }
  }

  test("recognize our refunds that we didn't keep") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt)
      assert(await(processor.step(f.swap())).progress.refundTxs.size == 1)
      // One of the refunds that we published before keeping only the last ones confirms.
      f.chain.clearMempool()
      val htlcOutput = f.htlcOutput()
      val oldRefund = Transaction(2, Seq(fr.acinq.bitcoin.scalacompat.TxIn(htlcOutput.outPoint, Nil, 0)), Seq(TxOut(htlcOutput.txOut.amount - 5000.sat, f.swap().contract.refundScript)), 0)
      f.chain.mine(oldRefund)
      assert(await(processor.step(f.swap())).state == States.HtlcPublished)
      f.chain.mineEmpty(LoopIn.RefundMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.FailedTimeout)
    }
  }

  test("resume swaps after a restart") {
    withFixture { f =>
      startSwap(f)
      val htlcTx = f.swap().progress.htlcTx_opt.get
      // We crashed before our htlc reached the mempool: we publish it again when restarting.
      f.chain.clearMempool()
      f.spawned = Nil
      f.manager.resumeSwaps()
      assert(f.spawned.map(_.swapHash) == Seq(f.swap().swapHash))
      assert(await(f.processor().step(f.spawned.head)).state == States.HtlcPublished)
      assert(f.chain.mempoolTxs == Seq(htlcTx))
      f.chain.mine()
      assert(serverPays(f))
      assert(await(f.processor().step(f.swap())).state == States.InvoiceSettled)
      // The key was pushed before the restart: we don't push it again.
      f.spawned = Nil
      f.manager.resumeSwaps()
      f.chain.mine(serverSuccessSweep(f))
      assert(await(f.processor().step(f.spawned.head)).state == States.Success)
      assert(f.server.requestsOfType[ServerPushKeyReq].size == 1)
      f.spawned = Nil
      f.manager.resumeSwaps()
      assert(f.spawned.isEmpty)
    }
  }

  test("process the swap again when our invoice is paid") {
    withFixture { f =>
      startSwap(f)
      val part = PaymentEvent.IncomingPayment(randomBytes32(), randomKey().publicKey, MilliSatoshi(995_000_000), TimestampMilli.now())
      assert(f.processor().onPaymentEvent(f.swap(), PaymentReceived(f.swap().swapHash, Seq(part))))
      assert(!f.processor().onPaymentEvent(f.swap(), PaymentReceived(randomBytes32(), Seq(part))))
    }
  }

  test("give the server route hints for our private channels when asked to") {
    withFixture { f =>
      val (peer1, peer2) = (randomKey().publicKey, randomKey().publicKey)
      val hop1 = ExtraHop(peer1, ShortChannelId(561), 1000 msat, 100, CltvExpiryDelta(40))
      val hop2 = ExtraHop(peer2, ShortChannelId(562), 0 msat, 1, CltvExpiryDelta(144))
      f.payments.privateChannelHints = Map(peer1 -> hop1, peer2 -> hop2)

      def sent(routeHints: java.util.List[RouteHint]): Set[Seq[ExtraHop]] = routeHints.asScala.map(_.getHopHintsList.asScala.toSeq.map { h =>
        ExtraHop(PublicKey(ByteVector.fromValidHex(h.getNodeId)), ShortChannelId(h.getChanId), MilliSatoshi(h.getFeeBaseMsat), h.getFeeProportionalMillionths, CltvExpiryDelta(h.getCltvExpiryDelta))
      }).toSet

      // Like loopd, the server gets our route hints in its probe and quote requests, and in our swap and probe invoices.
      def checkHints(swapHash: ByteVector32, expected: Set[Seq[ExtraHop]]): Unit = {
        assert(sent(f.server.requestsOfType[ServerProbeRequest].last.getRouteHintsList) == expected)
        assert(sent(f.server.requestsOfType[ServerLoopInQuoteRequest].last.getRouteHintsList) == expected)
        val swap = f.server.loopIns(swapHash)
        assert(swap.swapInvoice.routingInfo.toSet == expected)
        assert(swap.probeInvoice.routingInfo.toSet == expected)
      }

      checkHints(await(f.manager.loopIn(LoopInRequest(1_000_000 sat))).swapHash, Set.empty)
      checkHints(await(f.manager.loopIn(LoopInRequest(1_000_000 sat, routeHints = true))).swapHash, Set(Seq(hop1), Seq(hop2)))
      // only our channels with the last hop
      checkHints(await(f.manager.loopIn(LoopInRequest(1_000_000 sat, lastHop_opt = Some(peer2), routeHints = true))).swapHash, Set(Seq(hop2)))
      // quotes too
      await(f.manager.loopInQuote(1_000_000 sat, Some(peer1), routeHints = true))
      assert(sent(f.server.requestsOfType[ServerProbeRequest].last.getRouteHintsList) == Set(Seq(hop1)))
      assert(sent(f.server.requestsOfType[ServerLoopInQuoteRequest].last.getRouteHintsList) == Set(Seq(hop1)))
      await(f.manager.loopInQuote(1_000_000 sat, None))
      assert(f.server.requestsOfType[ServerProbeRequest].last.getRouteHintsCount == 0)
      assert(f.server.requestsOfType[ServerLoopInQuoteRequest].last.getRouteHintsCount == 0)
    }
  }

  test("abandon a swap before publishing our htlc") {
    withFixture { f =>
      await(f.manager.loopIn(LoopInRequest(1_000_000 sat)))
      val processor = f.processor()
      val abandoned = processor.abandon(f.swap()).toOption.get
      assert(abandoned.state == States.FailedAbandoned)
      assert(f.payments.invoices.isEmpty)
      assert(f.swap().state == States.FailedAbandoned)
    }
    withFixture { f =>
      val processor = startSwap(f)
      // Our funds are in the htlc: we must keep watching it to refund it.
      assert(processor.abandon(f.swap()).left.toOption.get.contains("refunded automatically"))
      assert(f.swap().state == States.HtlcPublished)
      f.chain.mine()
      assert(serverPays(f))
      assert(await(processor.step(f.swap())).state == States.InvoiceSettled)
      assert(processor.abandon(f.swap()).left.toOption.get.contains("already paid"))
    }
  }

  test("handle reorgs of our htlc and refund") {
    withFixture { f =>
      val processor = startSwap(f)
      val htlcTx = f.swap().progress.htlcTx_opt.get
      f.chain.mine()
      // Our htlc is reorganized out (and dropped from the mempool): we publish it again.
      f.chain.disconnectTip()
      f.chain.mineEmpty(1)
      assert(f.chain.mempoolTxs.isEmpty)
      assert(await(processor.step(f.swap())).state == States.HtlcPublished)
      assert(f.chain.mempoolTxs == Seq(htlcTx))
      f.chain.mine()
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt)
      val refund = await(processor.step(f.swap())).progress.refundTxs.loneElement
      f.chain.mine()
      // Our refund is reorganized out: we don't consider the swap done and publish it again.
      f.chain.mineEmpty(LoopIn.RefundMinDepth - 2)
      f.chain.disconnectTip()
      f.chain.disconnectTip()
      f.chain.mineEmpty(2)
      assert(await(processor.step(f.swap())).state == States.HtlcPublished)
      assert(f.chain.mempoolTxs == Seq(refund))
      f.chain.mine()
      f.chain.mineEmpty(LoopIn.RefundMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.FailedTimeout)
    }
  }

  test("keep pushing our internal key while the server is unreachable") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      assert(serverPays(f))
      f.server.unavailable = true
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.InvoiceSettled && !swap1.progress.keyPushed)
      f.server.unavailable = false
      f.chain.mineEmpty(1)
      assert(await(processor.step(f.swap())).progress.keyPushed)
      f.chain.mine(serverSuccessSweep(f))
      assert(await(processor.step(f.swap())).state == States.Success)
    }
  }

  test("refund if the server is unreachable after we published our htlc") {
    withFixture { f =>
      val processor = startSwap(f)
      f.server.unavailable = true
      f.chain.mine()
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt)
      assert(await(processor.step(f.swap())).progress.refundTxs.size == 1)
      f.chain.mine()
      f.chain.mineEmpty(LoopIn.RefundMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.FailedTimeout)
    }
  }

  test("encode and decode loop in data") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt)
      val swap = await(processor.step(f.swap()))
      assert(swap.progress.htlcTx_opt.nonEmpty && swap.progress.refundTxs.nonEmpty)
      assert(LoopInData.encode(swap.data).head == 2)
      assert(LoopInData.decode(LoopInData.encode(swap.data)) == swap.data)
      val withLastHop = swap.data.copy(contract = swap.contract.copy(lastHop_opt = Some(randomKey().publicKey)))
      assert(LoopInData.decode(LoopInData.encode(withLastHop)) == withLastHop)
      // We still read the swaps of earlier versions. v1 has one more byte, at the end of the contract: whether the swap
      // used the legacy internal key, which eclair-loop also set when re-encoding a v0 swap. We ignore it.
      val current = LoopInData.encode(swap.data).drop(1)
      val v1 = LoopInData.dataCodecV1.encode(swap.data).require.bytes
      assert(v1.size == current.size + 1)
      val flagIndex = current.toSeq.zip(v1.toSeq).indexWhere { case (b1, b2) => b1 != b2 }
      assert(v1(flagIndex) == 0 && v1.take(flagIndex) ++ v1.drop(flagIndex + 1) == current)
      assert(LoopInData.decode(ByteVector(1) ++ v1) == swap.data)
      assert(LoopInData.decode(ByteVector(1) ++ v1.update(flagIndex, 0xff.toByte)) == swap.data)
      val v0 = ByteVector(0) ++ LoopInData.dataCodecV0.encode(swap.data).require.bytes
      assert(LoopInData.decode(v0) == swap.data)
    }
  }

  test("we give the server a different internal key for each swap, even if we lost our database") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      assert(serverPays(f))
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.progress.keyPushed)
      val pushedKey = f.server.pushedKeys(swap1.swapHash)
      // We start again with an empty database: our next swap gets the same key index.
      val db = new SqliteLoopDb(DriverManager.getConnection("jdbc:sqlite::memory:"))
      try {
        val manager = new SwapManager(f.env.server, db, config, randomKey().publicKey, f.chain, f.payments, f.keys, Block.RegtestGenesisBlock.hash, _ => ())
        val info = await(manager.loopIn(LoopInRequest(1_000_000 sat)))
        assert(db.getSwap(info.swapHash).get.keyIndex == swap1.record.keyIndex)
        val request = f.server.requestsOfType[ServerLoopInRequest].last
        assert(request.getSenderKey.toByteArray.toSeq == f.keys.swapKey(swap1.record.keyIndex).publicKey.value.toArray.toSeq)
        // But the server doesn't know our internal key for this swap: it can't sweep our htlc without paying us.
        val internalKey = PublicKey(ByteVector(request.getSenderInternalPubkey.toByteArray))
        assert(internalKey == f.keys.internalKey(swap1.record.keyIndex, info.swapHash).publicKey)
        assert(internalKey != pushedKey.publicKey)
        assert(!f.server.loopIns(info.swapHash).htlc.internalKeys.contains(pushedKey.publicKey))
      } finally {
        db.close()
      }
    }
  }

  test("don't publish our htlc once our invoice is deleted") {
    withFixture { f =>
      val info = await(f.manager.loopIn(LoopInRequest(1_000_000 sat)))
      // The user deleted our invoice with eclair's deleteinvoice (or an abandon or a give-up removed it, but we couldn't
      // persist its final state): the server can't pay us anymore.
      assert(f.payments.removeInvoice(info.swapHash).isSuccess)
      val swap1 = await(f.processor().step(f.swap()))
      // Our funds would be locked in the htlc until its expiry, for nothing.
      assert(swap1.state == States.FailedAbandoned)
      assert(swap1.progress.htlcTx_opt.isEmpty)
      assert(f.chain.mempoolTxs.isEmpty)
      // We released the inputs we had funded it with.
      assert(f.chain.rolledBack.size == 1)
    }
  }

  test("release our htlc's inputs if we can't persist it, and fund it again") {
    withFixture { f =>
      await(f.manager.loopIn(LoopInRequest(1_000_000 sat)))
      val processor = f.processor()
      f.db.failNext("updateSwap")
      awaitFailure(processor.step(f.swap()))
      // We didn't publish it: we unlock its inputs.
      assert(f.swap().state == States.Initiated && f.swap().progress.htlcTx_opt.isEmpty)
      assert(f.chain.mempoolTxs.isEmpty)
      val unpublished = f.chain.rolledBack.loneElement
      assert(await(processor.step(f.swap())).state == States.HtlcPublished)
      assert(f.chain.mempoolTxs.loneElement != unpublished)
    }
  }

  test("abandoning a swap that we can't persist leaves it as it was") {
    withFixture { f =>
      val info = await(f.manager.loopIn(LoopInRequest(1_000_000 sat)))
      val processor = f.processor()
      f.db.failNext("updateSwap")
      assert(scala.util.Try(processor.abandon(f.swap())).isFailure)
      // Our invoice is gone but we're still in our initial state: we must not publish our htlc.
      assert(f.payments.incomingStatus(info.swapHash) == IncomingInvoiceStatus.Unknown)
      assert(await(processor.step(f.swap())).state == States.FailedAbandoned)
      assert(f.chain.mempoolTxs.isEmpty)
    }
  }

  test("report the fee of the refund that confirmed") {
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt)
      val refund1 = await(processor.step(f.swap())).progress.refundTxs.loneElement
      // We bump our refund, but the first one confirms.
      f.chain.feerates = _ => FeeratePerKw(5000 sat)
      f.chain.mineEmpty(1)
      val refund2 = await(processor.step(f.swap())).progress.refundTxs.last
      assert(refund2 != refund1)
      f.chain.clearMempool()
      f.chain.mine(refund1)
      f.chain.mineEmpty(LoopIn.RefundMinDepth - 1)
      val swap1 = await(processor.step(f.swap()))
      assert(swap1.state == States.FailedTimeout)
      val info = f.manager.getSwap(swap1.swapHash).get.loopIn.get
      assert(info.refundFee.contains(1_000_000.sat - refund1.txOut.head.amount))
      assert(info.refundTxIds.last == refund1.txid)
    }
    withFixture { f =>
      val processor = startSwap(f)
      f.chain.mine()
      f.chain.mineEmpty((f.swap().cltvExpiry - f.chain.height).toInt)
      assert(await(processor.step(f.swap())).progress.refundTxs.size == 1)
      // One of the refunds that we published before keeping only the last ones confirms.
      f.chain.clearMempool()
      val htlcOutput = f.htlcOutput()
      val oldRefund = Transaction(2, Seq(fr.acinq.bitcoin.scalacompat.TxIn(htlcOutput.outPoint, Nil, 0)), Seq(TxOut(htlcOutput.txOut.amount - 5000.sat, f.swap().contract.refundScript)), 0)
      f.chain.mine(oldRefund)
      f.chain.mineEmpty(LoopIn.RefundMinDepth - 1)
      assert(await(processor.step(f.swap())).state == States.FailedTimeout)
      val info = f.manager.getSwap(f.swap().swapHash).get.loopIn.get
      assert(info.refundFee.contains(5000.sat))
      assert(info.refundTxIds.last == oldRefund.txid)
    }
  }

  test("refuse confirmation targets we can't store before creating the swap") {
    withFixture { f =>
      assert(awaitFailure(f.manager.loopIn(LoopInRequest(1_000_000 sat, htlcConfTarget_opt = Some(70_000)))).getMessage.contains("confirmation target must be at most"))
      assert(awaitFailure(f.manager.loopInQuote(1_000_000 sat, None, htlcConfTarget_opt = Some(70_000))).getMessage.contains("confirmation target must be at most"))
      assert(f.server.loopIns.isEmpty)
      assert(f.payments.invoices.isEmpty)
    }
  }

  test("delete our invoice if we can't persist a swap that the server created") {
    withFixture { f =>
      f.db.failNext("addSwap")
      awaitFailure(f.manager.loopIn(LoopInRequest(1_000_000 sat)))
      assert(f.server.loopIns.size == 1)
      assert(f.payments.invoices.isEmpty)
      assert(f.db.listSwaps().isEmpty)
    }
  }
  test("create a loop in once per idempotency key") {
    withFixture { f =>
      val request = LoopInRequest(1_000_000 sat, idempotencyKey_opt = Some("deposit-7"))
      val info = await(f.manager.loopIn(request))
      assert(info.idempotencyKey.contains("deposit-7"))
      // A retry doesn't create another swap, nor another invoice.
      assert(await(f.manager.loopIn(request)).swapHash == info.swapHash)
      assert(f.server.loopIns.size == 1 && f.payments.invoices.keySet == Set(info.swapHash) && f.db.listSwaps().size == 1 && f.spawned.size == 1)
      // The key can't be used for another request, of either swap type.
      assert(awaitFailure(f.manager.loopIn(request.copy(routeHints = true))).getMessage == "idempotency key deposit-7 was already used for another request")
      assert(awaitFailure(f.manager.loopOut(LoopOutRequest(1_000_000 sat, idempotencyKey_opt = Some("deposit-7")))).getMessage == "idempotency key deposit-7 was already used for another request")
      assert(f.server.loopIns.size == 1)
    }
  }

}
