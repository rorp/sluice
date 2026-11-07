package io.github.rorp.sluice

import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.typed.{ActorRef, Scheduler}
import akka.testkit.TestKit
import akka.util.Timeout
import fr.acinq.bitcoin.scalacompat.{ByteVector32, SatoshiLong}
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import fr.acinq.eclair.payment.PaymentEvent
import io.github.rorp.sluice.SwapManager.{LoopInRequest, LoopOutRequest}
import io.github.rorp.sluice.db.{SwapRecord, SwapType}
import fr.acinq.eclair.{BlockHeight, TimestampMilli, randomBytes32}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}
import scodec.bits.ByteVector

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** The swap actor, spawned like the plugin does: a typed actor on eclair's classic actor system. */
class SwapActorSpec extends AnyFunSuite with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  case class TestSwap(record: SwapRecord) extends SwapLike

  private def testSwap(state: String = "RUNNING"): TestSwap = TestSwap(SwapRecord(randomBytes32(), SwapType.LoopOut, state, 100_000 sat, ByteVector32.Zeroes, 0, BlockHeight(800_000), ByteVector.empty, TimestampMilli.now(), TimestampMilli.now()))

  /**
   * Its methods throw instead of returning a failed future (like a db error before its first future) the first times
   * they're called.
   */
  class ThrowingProcessor(stepThrows: Int = 0, stepFails: Int = 0, reloadThrows: Int = 0, abandonThrows: Int = 0) extends SwapProcessor[TestSwap] {
    val steps = new AtomicInteger(0)
    /** The state of the swaps we processed. */
    val processed = new ConcurrentLinkedQueue[String]()
    private val reloads = new AtomicInteger(0)
    private val abandons = new AtomicInteger(0)

    override def step(swap: TestSwap): Future[TestSwap] = {
      processed.add(swap.record.state)
      val n = steps.incrementAndGet()
      if (n <= stepThrows) throw new RuntimeException("db is unavailable")
      else if (n <= stepThrows + stepFails) Future.failed(new RuntimeException("db is unavailable"))
      else Future.successful(swap)
    }

    override def reload(swap: TestSwap): TestSwap = {
      if (reloads.incrementAndGet() <= reloadThrows) throw new RuntimeException("db is unavailable")
      swap.copy(record = swap.record.copy(state = "RELOADED"))
    }

    override def isFinal(swap: TestSwap): Boolean = false

    override def onPaymentEvent(swap: TestSwap, event: PaymentEvent): Boolean = false

    override def abandon(swap: TestSwap): Either[String, TestSwap] = {
      if (abandons.incrementAndGet() <= abandonThrows) throw new RuntimeException("db is unavailable")
      Left("can't abandon")
    }
  }

  private def withSystem(test: akka.actor.ActorSystem => Unit): Unit = {
    val system = akka.actor.ActorSystem(s"swap-actor-spec-${randomBytes32().toHex.take(8)}")
    try test(system) finally TestKit.shutdownActorSystem(system)
  }

  test("keep running a swap whose processing step throws") {
    withSystem { system =>
      val processor = new ThrowingProcessor(stepThrows = 1)
      system.spawn(SwapActor(testSwap(), _ => processor, tickInterval = 100.millis), "swap")
      eventually(assert(processor.steps.get() >= 3))
    }
  }

  test("reload the swap from the db before processing it again after an error") {
    withSystem { system =>
      // Our step failed, and we can't reload the swap right away: we may have persisted part of its changes.
      val processor = new ThrowingProcessor(stepFails = 1, reloadThrows = 2)
      system.spawn(SwapActor(testSwap(), _ => processor, tickInterval = 100.millis), "swap")
      eventually(assert(processor.steps.get() >= 3))
      // We never process again the swap we had before the error: we wait until we can reload it.
      assert(processor.processed.asScala.toSeq.tail.forall(_ == "RELOADED"))
    }
  }

  test("answer abandon requests when abandoning throws") {
    withSystem { system =>
      implicit val timeout: Timeout = Timeout(5.seconds)
      implicit val scheduler: Scheduler = system.toTyped.scheduler
      val processor = new ThrowingProcessor(abandonThrows = 1)
      val actor: ActorRef[SwapActor.Command] = system.spawn(SwapActor(testSwap(), _ => processor, tickInterval = 100.millis), "swap")
      eventually(assert(processor.steps.get() >= 1))
      val reply1 = Await.result(actor.ask[Either[String, SwapRecord]](SwapActor.Abandon), 10.seconds)
      assert(reply1.left.toOption.exists(_.contains("db is unavailable")))
      // The swap keeps running, from the db's version of it (abandoning may have persisted part of its changes).
      val steps = processor.steps.get()
      eventually(assert(processor.steps.get() > steps))
      eventually(assert(processor.processed.asScala.lastOption.contains("RELOADED")))
      assert(Await.result(actor.ask[Either[String, SwapRecord]](SwapActor.Abandon), 10.seconds) == Left("can't abandon"))
    }
  }

  test("keep running a loop in while eclair's payments db is unavailable") {
    new LoopInSpec().withFixture { f =>
      withSystem { system =>
        Await.result(f.manager.loopIn(LoopInRequest(1_000_000 sat)), 10.seconds)
        // Checking whether the server paid us throws.
        f.payments.incomingStatusFailures = 1
        system.spawn(SwapActor(f.spawned.head, ec => new LoopInProcessor(f.env)(ec), tickInterval = 100.millis), "loop-in")
        eventually(assert(f.swap().state == LoopIn.States.HtlcPublished))
        assert(f.payments.incomingStatusFailures == 0)
      }
    }
  }

  test("keep sweeping a loop out while our db is unavailable") {
    new LoopOutSpec().withFixture { f =>
      withSystem { system =>
        Await.result(f.manager.loopOut(LoopOutRequest(1_000_000 sat)), 10.seconds)
        system.spawn(SwapActor(f.spawned.head, ec => new LoopOutProcessor(f.env)(ec), tickInterval = 100.millis), "loop-out")
        eventually(assert(f.payments.paid.size == 2))
        f.chain.mine(FakeLoopChain.fundingTx(f.htlc().pubkeyScriptBytes, 1_000_000 sat))
        eventually(assert(f.swap().state == LoopOut.States.PreimageRevealed && f.chain.mempoolTxs.size == 1))
        // Our db is unavailable while we bump our sweep: we can't persist it, nor reload the swap.
        f.db.failNext("updateSwap", 3)
        f.db.failNext("getSwap", 3)
        f.chain.clearMempool()
        f.chain.feerates = _ => FeeratePerKw(10_000 sat)
        f.chain.mineEmpty(1)
        // We keep going once it's back.
        eventually(assert(f.chain.mempoolTxs.size == 1))
        f.chain.mine()
        f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
        eventually(assert(f.swap().state == LoopOut.States.Success))
      }
    }
  }

}
