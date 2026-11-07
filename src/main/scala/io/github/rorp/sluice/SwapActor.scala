package io.github.rorp.sluice

import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import fr.acinq.eclair.BlockHeight
import fr.acinq.eclair.blockchain.CurrentBlockHeight
import fr.acinq.eclair.payment.PaymentEvent
import io.github.rorp.sluice.db.SwapRecord

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success, Try}

/** A swap and its decoded contract. */
trait SwapLike {
  def record: SwapRecord
}

/** The logic of a swap: each step looks at the chain and our payments, and moves the swap forward. */
trait SwapProcessor[S <: SwapLike] {
  def step(swap: S): Future[S]

  /** Reloads the swap from the db (after a failed step, which may have persisted part of its changes). */
  def reload(swap: S): S

  def isFinal(swap: S): Boolean

  /** Whether this payment event concerns the swap (it is then processed again right away). */
  def onPaymentEvent(swap: S, event: PaymentEvent): Boolean

  /** Stops the swap at the user's request (and persists it), if that is safe in its current state. */
  def abandon(swap: S): Either[String, S]
}

/**
 * Runs a swap: its processor is called on every block, on payment events concerning the swap and periodically, until
 * the swap reaches a final state. Processing steps never run concurrently.
 *
 * The processor's methods may throw instead of returning a failed future (a db error before their first future, for
 * example): the actor would stop, and nothing would run the swap until eclair restarts, so we handle those like failed
 * steps. After an error, the db has the swap's latest state (an error may come after persisting part of its changes):
 * we don't process or abandon the swap again until we could reload it.
 */
object SwapActor {

  // @formatter:off
  sealed trait Command
  /** Asks the swap to check its progress now. */
  case object Process extends Command
  case class GetState(replyTo: ActorRef[SwapRecord]) extends Command
  /** Abandons the swap if possible: replies with the reason why we can't, or the abandoned swap. */
  case class Abandon(replyTo: ActorRef[Either[String, SwapRecord]]) extends Command
  private case class WrappedBlockHeight(blockHeight: BlockHeight) extends Command
  private case class WrappedPaymentEvent(event: PaymentEvent) extends Command
  private case class ProcessDone(result: Try[SwapLike]) extends Command
  private case object Tick extends Command
  // @formatter:on

  /** @param makeProcessor creates the swap's processor, which runs on the actor's execution context. */
  def apply[S <: SwapLike](swap: S, makeProcessor: ExecutionContext => SwapProcessor[S], tickInterval: FiniteDuration = 1.minute): Behavior[Command] = Behaviors.setup { context =>
    val processor = makeProcessor(context.executionContext)
    context.system.eventStream ! EventStream.Subscribe(context.messageAdapter[CurrentBlockHeight](e => WrappedBlockHeight(e.blockHeight)))
    context.system.eventStream ! EventStream.Subscribe(context.messageAdapter[PaymentEvent](WrappedPaymentEvent))
    Behaviors.withTimers { timers =>
      timers.startTimerWithFixedDelay(Tick, tickInterval)
      context.self ! Process
      val swapHash = swap.record.swapHash
      /** An error may have left the db with a newer version of the swap than ours, that we couldn't reload yet. */
      var stale = false

      /** The swap as the db has it, if we may not have its latest version. */
      def current(s: S): Try[S] = if (stale) Try(processor.reload(s)).map { s1 => stale = false; s1 } else Success(s)

      def startStep(s: S): Unit = {
        val result = current(s).flatMap(s1 => Try(processor.step(s1))) match {
          case Success(f) => f
          case Failure(t) => Future.failed(t)
        }
        context.pipeToSelf(result)(r => ProcessDone(r))
      }

      /** After an error: the swap as the db has it if we can reload it, otherwise we'll reload it before using it again. */
      def reloaded(s: S): S = Try(processor.reload(s)) match {
        case Success(s1) =>
          stale = false
          s1
        case Failure(t) =>
          context.log.warn(s"swap $swapHash: cannot reload swap: ${t.getMessage}")
          stale = true
          s
      }

      /**
       * @param running a processing step is in progress.
       * @param rerun   something happened while processing: we must process again.
       */
      def abandon(swap: S, replyTo: ActorRef[Either[String, SwapRecord]], rerun: Boolean): Behavior[Command] = {
        val (swap1, result) = current(swap).flatMap(s => Try(processor.abandon(s)).map(r => (s, r))) match {
          case Success((s, r)) => (s, r)
          case Failure(t) =>
            context.log.warn(s"swap $swapHash: error while abandoning: ${t.getMessage}")
            (reloaded(swap), Left(s"error while abandoning: ${t.getMessage}"))
        }
        result match {
          case Left(reason) =>
            replyTo ! Left(reason)
            if (processor.isFinal(swap1)) {
              context.log.info(s"swap $swapHash: done (${swap1.record.state})")
              Behaviors.stopped
            } else if (rerun) {
              startStep(swap1)
              run(swap1, running = true, rerun = false, pendingAbandon = Nil)
            } else {
              run(swap1, running = false, rerun = false, pendingAbandon = Nil)
            }
          case Right(abandoned) =>
            context.log.info(s"swap $swapHash: abandoned")
            replyTo ! Right(abandoned.record)
            Behaviors.stopped
        }
      }

      /**
       * @param pendingAbandon abandon requests received while processing: we handle them once the step is done.
       */
      def run(swap: S, running: Boolean, rerun: Boolean, pendingAbandon: List[ActorRef[Either[String, SwapRecord]]]): Behavior[Command] = Behaviors.receiveMessage {
        case Process | Tick | _: WrappedBlockHeight =>
          if (running) {
            run(swap, running = true, rerun = true, pendingAbandon)
          } else {
            startStep(swap)
            run(swap, running = true, rerun = false, pendingAbandon)
          }
        case WrappedPaymentEvent(e) =>
          // If we can't tell, we process the swap again: that's always safe.
          if (Try(processor.onPaymentEvent(swap, e)).getOrElse(true)) context.self ! Process
          Behaviors.same
        case ProcessDone(result) =>
          val swap1 = result match {
            case Success(s) => s.asInstanceOf[S]
            case Failure(t) =>
              context.log.warn(s"swap $swapHash: error while processing: ${t.getMessage}")
              reloaded(swap)
          }
          if (processor.isFinal(swap1)) {
            context.log.info(s"swap $swapHash: done (${swap1.record.state})")
            pendingAbandon.foreach(_ ! Left(s"swap is already final (${swap1.record.state})"))
            Behaviors.stopped
          } else if (pendingAbandon.nonEmpty) {
            pendingAbandon.tail.foreach(_ ! Left("swap is being abandoned"))
            abandon(swap1, pendingAbandon.head, rerun)
          } else if (rerun) {
            startStep(swap1)
            run(swap1, running = true, rerun = false, Nil)
          } else {
            run(swap1, running = false, rerun = false, Nil)
          }
        case GetState(replyTo) =>
          replyTo ! swap.record
          Behaviors.same
        case Abandon(replyTo) =>
          if (running) run(swap, running = true, rerun = rerun, pendingAbandon :+ replyTo)
          else abandon(swap, replyTo, rerun = false)
      }

      run(swap, running = false, rerun = false, Nil)
    }
  }

}
