package io.github.rorp.sluice

import fr.acinq.eclair.NotificationsLogger
import fr.acinq.eclair.NotificationsLogger.NotifyNodeOperator
import grizzled.slf4j.Logging
import io.github.rorp.sluice.db.{SwapRecord, SwapType}

import scala.util.{Failure, Success, Try}

/**
 * Which swap state transitions we report to the node operator.
 *
 * @param loopOutStates we notify when a loop out enters one of these states (empty: never).
 * @param loopInStates  we notify when a loop in enters one of these states (empty: never).
 */
case class NotificationsConfig(loopOutStates: Set[String], loopInStates: Set[String]) {
  def states(swapType: SwapType): Set[String] = swapType match {
    case SwapType.LoopOut => loopOutStates
    case SwapType.LoopIn => loopInStates
  }
}

object NotificationsConfig {
  val All: NotificationsConfig = NotificationsConfig(LoopOut.States.all.toSet, LoopIn.States.all.toSet)
  val Disabled: NotificationsConfig = NotificationsConfig(Set.empty, Set.empty)
}

/**
 * Reports swap state transitions to the node operator with eclair's [[NotifyNodeOperator]] events: eclair's
 * NotificationsLogger writes them to its notifications log, and other plugins can subscribe to them.
 *
 * @param publish publishes the event on eclair's event stream.
 */
class SwapNotifier(config: NotificationsConfig, publish: NotifyNodeOperator => Unit) extends Logging {

  /**
   * Called once the new state of the swap is persisted: a failure to notify never fails the swap's processing.
   *
   * @param from_opt   the previous state, None for a new swap.
   * @param notice_opt why the swap failed, if it did: a fixed description, without amounts, txids, channels or other
   *                   data about the swap, which notifications may be forwarded to other services.
   */
  def stateChanged(record: SwapRecord, from_opt: Option[String], notice_opt: Option[String] = None): Unit =
    if (config.states(record.swapType).contains(record.state)) {
      Try(publish(SwapNotifier.notification(record, from_opt, notice_opt))) match {
        case Success(_) => ()
        case Failure(t) => logger.warn(s"swap ${record.swapHash}: cannot notify the node operator: ${t.getMessage}")
      }
    }

}

object SwapNotifier {

  val Disabled = new SwapNotifier(NotificationsConfig.Disabled, _ => ())

  /** Failures that lost funds are errors, other failures are warnings. */
  def severity(swapType: SwapType, state: String): NotificationsLogger.Severity = (swapType, state) match {
    // The server reclaimed its htlc after we revealed the preimage: it can take our swap payment.
    case (SwapType.LoopOut, LoopOut.States.FailedSweepTimeout) => NotificationsLogger.Error
    case (SwapType.LoopOut, LoopOut.States.FailedInvalid) => NotificationsLogger.Error
    case (_, s) if s.startsWith("FAILED_") => NotificationsLogger.Warning
    case _ => NotificationsLogger.Info
  }

  def notification(record: SwapRecord, from_opt: Option[String], notice_opt: Option[String]): NotifyNodeOperator = {
    val swapType = record.swapType match {
      case SwapType.LoopOut => "loop out"
      case SwapType.LoopIn => "loop in"
    }
    val transition = from_opt match {
      case Some(from) => s"$from -> ${record.state}"
      case None => s"created (${record.state})"
    }
    val message = s"sluice: $swapType ${record.swapHash}: $transition${notice_opt.map(n => s": $n").getOrElse("")}"
    NotifyNodeOperator(severity(record.swapType, record.state), message)
  }

}
