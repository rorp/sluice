package io.github.rorp.sluice

import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import fr.acinq.bitcoin.scalacompat.SatoshiLong
import fr.acinq.eclair.blockchain.CurrentBlockHeight
import io.github.rorp.sluice.LoopOut.States
import io.github.rorp.sluice.SwapManager.LoopOutRequest
import io.github.rorp.sluice.db.SwapRecord
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class LoopOutActorSpec extends ScalaTestWithActorTestKit with AnyFunSuiteLike {

  private val helper = new LoopOutSpec()

  test("abandon a running swap") {
    helper.withFixture { f =>
      Await.result(f.manager.loopOut(LoopOutRequest(1_000_000 sat)), 10 seconds)
      val actor = testKit.spawn(LoopOut(f.spawned.head, f.env))
      val probe = TestProbe[Either[String, SwapRecord]]()
      eventually(assert(f.payments.paid.size == 2))
      actor ! SwapActor.Abandon(probe.ref)
      assert(probe.expectMessageType[Right[String, SwapRecord]].value.state == States.FailedAbandoned)
      probe.expectTerminated(actor)
      assert(f.swap().state == States.FailedAbandoned)
      // Final swaps can't be abandoned.
      assert(Await.result(f.manager.abandonSwap(f.swap().swapHash).failed, 10 seconds).getMessage.contains("already final"))
    }
  }

  test("refuse to abandon a swap after revealing the preimage") {
    helper.withFixture { f =>
      Await.result(f.manager.loopOut(LoopOutRequest(1_000_000 sat)), 10 seconds)
      f.chain.mine(FakeLoopChain.fundingTx(f.htlc().pubkeyScriptBytes, 1_000_000 sat))
      val actor = testKit.spawn(LoopOut(f.spawned.head, f.env))
      eventually(assert(f.swap().state == States.PreimageRevealed))
      val probe = TestProbe[Either[String, SwapRecord]]()
      actor ! SwapActor.Abandon(probe.ref)
      assert(probe.expectMessageType[Left[String, SwapRecord]].value.contains("preimage has been revealed"))
      // The swap keeps running.
      f.chain.mine()
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      system.eventStream ! akka.actor.typed.eventstream.EventStream.Publish(CurrentBlockHeight(f.chain.height))
      eventually(assert(f.swap().state == States.Success))
    }
  }

  test("run a loop out swap on new blocks until it completes") {
    helper.withFixture { f =>
      Await.result(f.manager.loopOut(LoopOutRequest(1_000_000 sat)), 10 seconds)
      val actor = testKit.spawn(LoopOut(f.spawned.head, f.env))
      val probe = TestProbe[SwapRecord]()

      def stateAfterBlock(): String = {
        system.eventStream ! akka.actor.typed.eventstream.EventStream.Publish(CurrentBlockHeight(f.chain.height))
        actor ! SwapActor.GetState(probe.ref)
        probe.expectMessageType[SwapRecord].state
      }

      // The swap pays the server when it starts.
      eventually(assert(f.payments.paid.size == 2))
      assert(stateAfterBlock() == States.Initiated)
      f.chain.mine(FakeLoopChain.fundingTx(f.htlc().pubkeyScriptBytes, 1_000_000 sat))
      system.eventStream ! akka.actor.typed.eventstream.EventStream.Publish(CurrentBlockHeight(f.chain.height))
      eventually(assert(f.chain.mempoolTxs.size == 1))
      eventually(assert(f.swap().state == States.PreimageRevealed))
      f.chain.mine()
      f.chain.mineEmpty(LoopOut.SweepMinDepth - 1)
      system.eventStream ! akka.actor.typed.eventstream.EventStream.Publish(CurrentBlockHeight(f.chain.height))
      eventually(assert(f.swap().state == States.Success))
      probe.expectTerminated(actor)
      // A running swap reports its state.
      val actor2 = testKit.spawn(LoopOut(LoopOut.Swap(f.db.getSwap(f.swap().swapHash).get.copy(state = States.Initiated)), f.env))
      actor2 ! SwapActor.GetState(probe.ref)
      assert(probe.expectMessageType[SwapRecord].swapHash == f.swap().swapHash)
    }
  }

}
