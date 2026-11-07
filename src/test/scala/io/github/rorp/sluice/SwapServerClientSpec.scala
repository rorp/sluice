package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.{Block, ByteVector32, Crypto, SatoshiLong}
import io.github.rorp.sluice.L402Payer.{Failed, Paid, PaymentStatus}
import io.github.rorp.sluice.SwapServerClient.{LoopInTerms, LoopOutTerms}
import io.github.rorp.sluice.db.{L402Token, LoopDb, SqliteLoopDb}
import io.github.rorp.sluice.swapserverrpc._
import fr.acinq.eclair.{BlockHeight, MilliSatoshi, TimestampMilli, TimestampSecond, randomBytes32, randomKey}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Server, Status, StatusRuntimeException}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.DriverManager
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

class SwapServerClientSpec extends AnyFunSuite {

  implicit val ec: ExecutionContext = ExecutionContext.global

  case class Fixture(server: FakeSwapServer, payer: FakeL402Payer, db: LoopDb, client: SwapServerClient, manager: SwapManager)

  private val currentHeight = BlockHeight(800_000)
  private val nodeId = randomKey().publicKey
  private val config = SluiceConfig("in-process", ServerTls.Insecure, "sluice/v0.1.0-test", 10 seconds, maxL402Cost = 1000 sat, maxL402RoutingFee = 10 sat, l402PaymentTimeout = 10 seconds, l402MinPurchaseInterval = 1 hour, defaultSweepConfTarget = 9, loopOutHtlcConfirmations = 1, maxSwapFeePpm = 20_000, maxPrepay = 30_000 sat)

  private def withFixture(server: FakeSwapServer, config: SluiceConfig = this.config)(test: Fixture => Unit): Unit = {
    val name = UUID.randomUUID().toString
    val grpcServer: Server = InProcessServerBuilder.forName(name).directExecutor().addService(server.serviceDefinition).build().start()
    val channel = InProcessChannelBuilder.forName(name).directExecutor().build()
    val db = new SqliteLoopDb(DriverManager.getConnection("jdbc:sqlite::memory:"))
    val payer = new FakeL402Payer(server)
    val l402 = new L402Authenticator(db, payer, Block.RegtestGenesisBlock.hash, config.maxL402Cost, config.maxL402RoutingFee, config.l402PaymentTimeout, config.l402MinPurchaseInterval)
    val client = new SwapServerClient(channel, l402, config.userAgent, config.rpcTimeout)
    val manager = new SwapManager(client, db, config, nodeId, new FakeLoopChain(currentHeight), new FakeLoopPayments(), new LoopKeys(randomBytes32().bytes), Block.RegtestGenesisBlock.hash, _ => ())
    try {
      test(Fixture(server, payer, db, client, manager))
    } finally {
      client.close()
      grpcServer.shutdownNow()
      db.close()
    }
  }

  private def withFixture(test: Fixture => Unit): Unit = withFixture(new FakeSwapServer())(test)

  private def await[T](f: Future[T]): T = Await.result(f, 10 seconds)

  private def awaitFailure(f: Future[_]): Throwable = Await.ready(f, 10 seconds).value.get.failed.get

  test("pay for an L402 token on first call and reuse it afterwards") {
    withFixture { f =>
      assert(f.db.currentL402Token().isEmpty)
      assert(await(f.client.loopOutTerms()) == LoopOutTerms(250_000 sat, 5_000_000 sat, 20, 60))
      assert(f.payer.payments.get() == 1)
      assert(f.payer.lastMaxFee.contains(10 sat))
      val token = f.db.currentL402Token().get
      assert(!token.isPending)
      assert(token.preimage_opt.map(Crypto.sha256(_)).contains(token.paymentHash))
      assert(token.amountPaid == MilliSatoshi(100_000))
      assert(token.routingFeePaid == MilliSatoshi(1000))
      // the token is reused for every call
      assert(await(f.client.loopInTerms()) == LoopInTerms(250_000 sat, 2_000_000 sat))
      await(f.client.fetchL402())
      assert(f.payer.payments.get() == 1)
      assert(f.server.challengesIssued.get() == 1)
      // we always send our protocol version and user agent
      val termsReq = f.server.requestsOfType[ServerLoopOutTermsRequest].head
      assert(termsReq.getProtocolVersion == ProtocolVersion.MUSIG2)
      assert(termsReq.getUserAgent == config.userAgent)
    }
  }

  test("concurrent calls pay for a single token") {
    withFixture { f =>
      val pending = f.payer.blockPayments()
      val calls = (1 to 5).map(_ => f.client.loopOutTerms())
      awaitCond(f.server.challengesIssued.get() == 5)
      pending.success(Paid(f.server.preimages(f.db.currentL402Token().get.paymentHash), MilliSatoshi(100_000), MilliSatoshi(0)))
      calls.foreach(c => assert(await(c).maxSwapAmount == 5_000_000.sat))
      assert(f.payer.payments.get() == 1)
      assert(f.db.listL402Tokens().size == 1)
    }
  }

  test("pay for a new token when the server rejects ours") {
    // We bought our token long enough ago.
    withFixture(new FakeSwapServer(), config.copy(l402MinPurchaseInterval = 0 seconds)) { f =>
      await(f.client.loopOutTerms())
      val token1 = f.db.currentL402Token().get
      f.server.revoked.put(token1.paymentHash, ())
      await(f.client.loopOutTerms())
      val token2 = f.db.currentL402Token().get
      assert(token2.paymentHash != token1.paymentHash)
      assert(f.payer.payments.get() == 2)
    }
  }

  test("don't pay for new tokens automatically when the server rejects a token we bought recently") {
    withFixture { f =>
      await(f.client.loopOutTerms())
      val token1 = f.db.currentL402Token().get
      // The server could make us pay on every call: our swaps call it every block and every minute.
      f.server.revoked.put(token1.paymentHash, ())
      val e = awaitFailure(f.client.loopOutTerms())
      assert(e.isInstanceOf[L402.TokenRejected] && e.getMessage.contains("use loopfetchl402"))
      assert(awaitFailure(f.client.loopInTerms()).isInstanceOf[L402.TokenRejected])
      assert(f.payer.payments.get() == 1)
      assert(f.db.currentL402Token().get.paymentHash == token1.paymentHash)
      // The user can still ask for a new token.
      await(f.client.fetchL402())
      assert(f.payer.payments.get() == 2)
      val token2 = f.db.currentL402Token().get
      assert(token2.paymentHash != token1.paymentHash && !token2.isPending)
      assert(await(f.client.loopOutTerms()) == LoopOutTerms(250_000 sat, 5_000_000 sat, 20, 60))
      f.server.revoked.put(token2.paymentHash, ())
      assert(awaitFailure(f.client.loopOutTerms()).isInstanceOf[L402.TokenRejected])
      assert(f.payer.payments.get() == 2)
    }
  }

  test("don't pay for tokens that are too expensive") {
    withFixture(new FakeSwapServer(l402Price = MilliSatoshi(1_000_001))) { f =>
      assert(awaitFailure(f.client.loopOutTerms()).isInstanceOf[L402.TokenTooExpensive])
      assert(f.payer.payments.get() == 0)
      assert(f.db.currentL402Token().isEmpty)
    }
  }

  test("remove pending token when its payment fails") {
    withFixture { f =>
      f.payer.result = _ => Future.successful(Failed("no route"))
      assert(awaitFailure(f.client.loopOutTerms()).isInstanceOf[L402.PaymentFailedError])
      assert(f.db.listL402Tokens().isEmpty)
      // the next call pays for a new token
      f.payer.result = invoice => Future.successful(Paid(f.server.preimages(invoice.paymentHash), invoice.amount_opt.get, MilliSatoshi(0)))
      await(f.client.loopOutTerms())
      assert(f.payer.payments.get() == 2)
    }
  }

  test("keep pending token when its payment times out and resume it later") {
    withFixture { f =>
      f.payer.result = _ => Future.failed(new java.util.concurrent.TimeoutException("payment timed out"))
      assert(awaitFailure(f.client.loopOutTerms()).isInstanceOf[java.util.concurrent.TimeoutException])
      val pending = f.db.currentL402Token().get
      assert(pending.isPending)
      // The payment is still in flight: we don't pay again.
      f.payer.statuses = Map(pending.paymentHash -> PaymentStatus.Pending)
      assert(awaitFailure(f.client.loopOutTerms()) == L402.PaymentInFlight(pending.paymentHash))
      assert(f.payer.payments.get() == 1)
      // The payment eventually succeeds: we use the pending token without paying again.
      val paid = Paid(f.server.preimages(pending.paymentHash), MilliSatoshi(100_000), MilliSatoshi(42))
      f.payer.statuses = Map(pending.paymentHash -> PaymentStatus.Succeeded(paid))
      await(f.client.loopOutTerms())
      assert(f.payer.payments.get() == 1)
      assert(f.db.currentL402Token().contains(pending.copy(preimage_opt = Some(paid.preimage), amountPaid = paid.amount, routingFeePaid = paid.fees)))
    }
  }

  test("pay for a new token if a pending token's payment failed") {
    withFixture { f =>
      val stale = L402Token(ByteVector32.One, ByteVector32.Zeroes.bytes, None, MilliSatoshi(0), MilliSatoshi(0), TimestampMilli.now() - 1.minute)
      f.db.storeL402Token(stale)
      f.payer.statuses = Map(stale.paymentHash -> PaymentStatus.Failed)
      await(f.client.loopOutTerms())
      assert(f.payer.payments.get() == 1)
      assert(f.db.listL402Tokens().map(_.paymentHash) == Seq(f.db.currentL402Token().get.paymentHash))
    }
  }

  test("loop out quote") {
    withFixture { f =>
      val before = TimestampSecond.now()
      val quote = await(f.manager.loopOutQuote(1_000_000 sat, confTarget_opt = Some(30), fast = false))
      assert(quote.swapFee == 10_000.sat)
      assert(quote.prepayAmount == 1337.sat)
      assert(quote.swapPaymentDest == f.server.swapPaymentDest)
      assert(quote.cltvExpiry == currentHeight + 30 + SwapManager.LoopOutExpiryMargin)
      assert(quote.swapPublicationDeadline >= before + 30.minutes)
      val req = f.server.requestsOfType[ServerLoopOutQuoteRequest].last
      assert(req.getAmt == 1_000_000)
      assert(req.getExpiry == quote.cltvExpiry.toLong)
      assert(req.getSwapPublicationDeadline == quote.swapPublicationDeadline.toLong)
      assert(req.getProtocolVersion == ProtocolVersion.MUSIG2)
      // a conf target below the server's minimum cltv delta uses that minimum
      assert(await(f.manager.loopOutQuote(1_000_000 sat, confTarget_opt = None, fast = true)).cltvExpiry == currentHeight + 20 + SwapManager.LoopOutExpiryMargin)
      // invalid parameters
      assert(awaitFailure(f.manager.loopOutQuote(1_000_000 sat, confTarget_opt = Some(61), fast = false)).getMessage.contains("exceeds maximum server cltv delta"))
      assert(awaitFailure(f.manager.loopOutQuote(1_000_000 sat, confTarget_opt = Some(1), fast = false)).getMessage.contains("confirmation target must be at least 2"))
      assert(awaitFailure(f.manager.loopOutQuote(249_999 sat, confTarget_opt = None, fast = false)).getMessage.contains("swap amount too low"))
      assert(awaitFailure(f.manager.loopOutQuote(5_000_001 sat, confTarget_opt = None, fast = false)).getMessage.contains("swap amount too high"))
    }
  }

  test("loop in quote") {
    withFixture { f =>
      val lastHop = randomKey().publicKey
      val quote = await(f.manager.loopInQuote(1_000_000 sat, Some(lastHop)))
      assert(quote.swapFee == 5000.sat)
      assert(quote.cltvDelta == 1000)
      val probe = f.server.requestsOfType[ServerProbeRequest].last
      assert(probe.getAmt == 1_000_000)
      assert(probe.getTarget.toByteArray.toSeq == nodeId.value.toArray.toSeq)
      assert(probe.getLastHop.toByteArray.toSeq == lastHop.value.toArray.toSeq)
      val req = f.server.requestsOfType[ServerLoopInQuoteRequest].last
      assert(req.getPubkey.toByteArray.toSeq == nodeId.value.toArray.toSeq)
      assert(req.getLastHop.toByteArray.toSeq == lastHop.value.toArray.toSeq)
      // probe failures are ignored
      f.server.probeStatus = Some(Status.INTERNAL.withDescription("probe failed"))
      assert(await(f.manager.loopInQuote(1_000_000 sat, None)).swapFee == 5000.sat)
      assert(!f.server.requestsOfType[ServerLoopInQuoteRequest].last.hasField(ServerLoopInQuoteRequest.getDescriptor.findFieldByName("last_hop")))
      assert(awaitFailure(f.manager.loopInQuote(2_000_001 sat, None)).getMessage.contains("swap amount too high"))
    }
  }

  test("server errors are propagated") {
    withFixture { f =>
      f.server.probeStatus = Some(Status.FAILED_PRECONDITION.withDescription("nope"))
      val e = awaitFailure(f.client.probe(1_000_000 sat, nodeId, None))
      assert(e.isInstanceOf[StatusRuntimeException])
      assert(e.asInstanceOf[StatusRuntimeException].getStatus.getCode == Status.Code.FAILED_PRECONDITION)
    }
  }

  private def awaitCond(p: => Boolean): Unit = {
    val deadline = System.currentTimeMillis() + 10_000
    while (!p) {
      if (System.currentTimeMillis() > deadline) fail("condition not met")
      Thread.sleep(10)
    }
  }

}
