package io.github.rorp.sluice

import akka.actor.typed.Behavior
import fr.acinq.bitcoin.ScriptFlags
import fr.acinq.bitcoin.scalacompat.Crypto.PrivateKey
import fr.acinq.bitcoin.scalacompat.{ByteVector32, OutPoint, Satoshi, Transaction}
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import fr.acinq.eclair.payment.{PaymentEvent, PaymentReceived}
import io.github.rorp.sluice.db.SwapRecord
import fr.acinq.eclair.transactions.Transactions
import fr.acinq.eclair.{BlockHeight, TimestampMilli}
import grizzled.slf4j.Logging

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * A loop in swap (on-chain -> off-chain), ported from loopd's loopin.go:
 *  1. we create an invoice for the swap amount minus the server's fee, with our own preimage;
 *  2. we lock the swap amount in an on-chain htlc that the server can claim with the preimage, and that we can refund
 *     after its expiry;
 *  3. once the htlc is confirmed, the server pays our invoice, which reveals the preimage; we then give the server our
 *     MuSig2 internal key so that it can sweep the htlc through the key path;
 *  4. if the server doesn't pay us before the htlc expires, we delete our invoice and refund the htlc.
 *
 * Once our invoice has been paid we never refund the htlc.
 */
object LoopIn {

  // @formatter:off
  object States {
    /** Publishing our htlc. */
    val Initiated = "INITIATED"
    /** Our htlc is published: waiting for the server to pay us, or for the htlc to expire (we then refund it). */
    val HtlcPublished = "HTLC_PUBLISHED"
    /** The server paid us: waiting for it to sweep the htlc. */
    val InvoiceSettled = "INVOICE_SETTLED"
    val Success = "SUCCESS"
    /** The server didn't pay us in time: our htlc was refunded, or never published. */
    val FailedTimeout = "FAILED_TIMEOUT"
    /** We couldn't fund our htlc. */
    val FailedInsufficientFunds = "FAILED_INSUFFICIENT_FUNDS"
    /** The user abandoned the swap before we published our htlc. */
    val FailedAbandoned = "FAILED_ABANDONED"
  }
  // @formatter:on

  def isFinal(state: String): Boolean = state != States.Initiated && state != States.HtlcPublished && state != States.InvoiceSettled

  // Values from loopd (loopin.go, loopout.go).
  /** We don't accept an htlc that would lock our funds for more blocks than this. */
  val MaxLoopInAcceptDelta = 1500
  /** We don't accept an htlc expiring in less blocks than this. */
  val MinLoopInExpiryDelta = 100
  /** We don't publish our htlc if it expires in less blocks than this: the server wouldn't have time to pay us. */
  val MinLoopInPublishDelta = 10
  val DefaultHtlcConfTarget = 6
  val RefundConfTarget = 2
  /** Confirmations of our refund before we consider the swap done. */
  val RefundMinDepth = 3
  /** We keep the last published refunds, in case a replaced one confirms (we recognize older ones by their output). */
  val MaxRefundTxs = 25

  case class Swap(record: SwapRecord, data: LoopInData) extends SwapLike {
    val swapHash: ByteVector32 = record.swapHash
    val state: String = record.state
    val amount: Satoshi = record.amount
    val cltvExpiry: BlockHeight = record.cltvExpiry
    val contract: LoopInContract = data.contract
    val progress: LoopInProgress = data.progress
  }

  object Swap {
    def apply(record: SwapRecord): Swap = Swap(record, LoopInData.decode(record.contract))
  }

  /** Our MuSig2 internal key, that we give to the server once it paid us. */
  def internalKey(swap: Swap, keys: LoopKeys): PrivateKey = keys.internalKey(swap.record.keyIndex, swap.swapHash)

  def htlc(swap: Swap, keys: LoopKeys): Htlc = Htlc(
    swap.swapHash,
    swap.cltvExpiry,
    senderInternalKey = internalKey(swap, keys).publicKey,
    receiverInternalKey = swap.contract.serverInternalKey,
    senderKey = keys.swapKey(swap.record.keyIndex).publicKey,
    receiverKey = swap.contract.serverKey)

  /** The htlc output of our htlc transaction. */
  def htlcOutput(swap: Swap, keys: LoopKeys): Option[HtlcOutput] = swap.progress.htlcTx_opt.flatMap { tx =>
    val script = htlc(swap, keys).pubkeyScriptBytes
    tx.txOut.zipWithIndex.collectFirst { case (o, i) if o.publicKeyScript == script => HtlcOutput(OutPoint(tx, i), o) }
  }

  def apply(swap: Swap, env: SwapEnv): Behavior[SwapActor.Command] = SwapActor(swap, ec => new LoopInProcessor(env)(ec))

}

/** The loop in logic: each step looks at the chain and our invoice, and moves the swap forward. */
class LoopInProcessor(env: SwapEnv)(implicit ec: ExecutionContext) extends SwapProcessor[LoopIn.Swap] with Logging {

  import LoopIn._

  /** The last height at which we published or re-broadcast our transactions. */
  @volatile private var lastPublishHeight_opt: Option[BlockHeight] = None

  override def onPaymentEvent(swap: Swap, event: PaymentEvent): Boolean = event match {
    case e: PaymentReceived => e.paymentHash == swap.swapHash
    case _ => false
  }

  override def reload(swap: Swap): Swap = env.db.getSwap(swap.swapHash).map(Swap(_)).getOrElse(swap)

  override def isFinal(swap: Swap): Boolean = LoopIn.isFinal(swap.state)

  private def log(swap: Swap, msg: String): Unit = logger.info(s"swap ${swap.swapHash}: $msg")

  override def step(swap: Swap): Future[Swap] = {
    val height = env.chain.currentHeight()
    // Whatever our state, once the server has paid us the swap is (nearly) done.
    val swap1 = if (isPaid(swap) && (swap.state == States.Initiated || swap.state == States.HtlcPublished)) update(swap, States.InvoiceSettled, swap.progress) else swap
    swap1.state match {
      case States.Initiated => publishHtlc(swap1, height)
      case States.HtlcPublished => waitForPayment(swap1, height)
      case States.InvoiceSettled => settled(swap1, height)
      case _ => Future.successful(swap1)
    }
  }

  private def update(swap: Swap, state: String, progress: LoopInProgress): Swap = {
    if (state != swap.state || progress != swap.progress) {
      if (state != swap.state) log(swap, s"${swap.state} -> $state")
      val data = swap.data.copy(progress = progress)
      val now = TimestampMilli.now()
      env.db.updateSwap(swap.swapHash, state, LoopInData.encode(data), now)
      swap.copy(record = swap.record.copy(state = state, contract = LoopInData.encode(data), updatedAt = now), data = data)
    } else {
      swap
    }
  }

  private def fail(swap: Swap, state: String, reason: String): Swap = {
    logger.warn(s"swap ${swap.swapHash}: failed: $reason")
    update(swap, state, swap.progress)
  }

  private def isPaid(swap: Swap): Boolean = env.payments.incomingStatus(swap.swapHash) match {
    case _: IncomingInvoiceStatus.Received => true
    case _ => false
  }

  /**
   * Deletes our invoice so that the server can't pay it anymore (and learn the preimage), which must happen before we
   * refund or give up on the htlc. Returns false if the invoice has been paid.
   */
  private def removeInvoice(swap: Swap): Boolean = env.payments.removeInvoice(swap.swapHash) match {
    case Success(_) => true
    case Failure(t) =>
      log(swap, s"cannot remove invoice: ${t.getMessage}")
      false
  }

  override def abandon(swap: Swap): Either[String, Swap] = swap.state match {
    case States.Initiated if removeInvoice(swap) => Right(fail(swap, States.FailedAbandoned, "abandoned by the user"))
    case States.Initiated => Left("the server already paid us")
    case States.HtlcPublished => Left("our htlc is published: it will be refunded automatically after its expiry if the server doesn't pay us")
    case States.InvoiceSettled => Left("the server already paid us")
    case state => Left(s"swap is already final ($state)")
  }

  // --- Publishing our htlc ---

  private def publishHtlc(swap: Swap, height: BlockHeight): Future[Swap] = {
    if (swap.cltvExpiry - height < MinLoopInPublishDelta) {
      if (removeInvoice(swap)) Future.successful(fail(swap, States.FailedTimeout, s"too late to publish the htlc (height=$height expiry=${swap.cltvExpiry})"))
      else Future.successful(swap)
    } else {
      val htlc = LoopIn.htlc(swap, env.keys)
      val feerate = env.chain.feerate(swap.contract.htlcConfTarget)
      env.chain.fundOutput(htlc.pubkeyScriptBytes, swap.amount, feerate, swap.contract.limits.maxMinerFee).transformWith {
        case Failure(t) =>
          if (removeInvoice(swap)) Future.successful(fail(swap, States.FailedInsufficientFunds, s"cannot fund htlc: ${t.getMessage}"))
          else Future.successful(swap)
        case Success((tx, fee)) =>
          // The server can only pay us while our invoice exists. It may have been deleted (with eclair's deleteinvoice,
          // or when we abandoned or gave up on the swap but couldn't persist its new state): our funds would then be
          // locked in the htlc until its expiry, for nothing.
          Try(isPayable(swap)).flatMap {
            case true =>
              // We persist our htlc before publishing it, so that we can always refund it.
              Try(update(swap, States.HtlcPublished, swap.progress.copy(htlcTx_opt = Some(tx), htlcTxFee = fee)))
            case false =>
              Success(fail(swap, States.FailedAbandoned, "our invoice was deleted before we published our htlc"))
          } match {
            case Success(swap1) if swap1.state == States.HtlcPublished =>
              lastPublishHeight_opt = Some(height)
              log(swap, s"publishing htlc ${tx.txid} (fee=$fee feerate=$feerate)")
              env.chain.publish(tx).map(_ => swap1).recover {
                case t =>
                  logger.warn(s"swap ${swap.swapHash}: cannot publish htlc ${tx.txid}: ${t.getMessage}")
                  swap1
              }
            case result =>
              // We won't publish this transaction: we unlock its inputs, so that our wallet can use them again.
              env.chain.rollback(tx).failed.foreach(t => logger.warn(s"swap ${swap.swapHash}: cannot unlock htlc inputs: ${t.getMessage}"))
              Future.fromTry(result)
          }
      }
    }
  }

  /** Whether the server can still pay our invoice (once it has, the swap is settled). */
  private def isPayable(swap: Swap): Boolean = env.payments.incomingStatus(swap.swapHash) != IncomingInvoiceStatus.Unknown

  /** Re-broadcasts our htlc once per block until it confirms. */
  private def rebroadcastHtlc(swap: Swap, height: BlockHeight): Future[Boolean] = {
    val tx = swap.progress.htlcTx_opt.get
    if (lastPublishHeight_opt.contains(height)) {
      Future.successful(true)
    } else {
      lastPublishHeight_opt = Some(height)
      env.chain.publish(tx).map(_ => true).recover {
        case t =>
          logger.warn(s"swap ${swap.swapHash}: cannot publish htlc ${tx.txid}: ${t.getMessage}")
          false
      }
    }
  }

  // --- Waiting for the server to pay us, or refunding ---

  private def waitForPayment(swap: Swap, height: BlockHeight): Future[Swap] = {
    val htlcOutput = LoopIn.htlcOutput(swap, env.keys).get
    env.chain.getTxConfirmations(htlcOutput.outPoint.txid).flatMap {
      case Some(confirmations) if confirmations > 0 =>
        if (height >= swap.cltvExpiry) refund(swap, htlcOutput, confirmations, height) else Future.successful(swap)
      case _ =>
        rebroadcastHtlc(swap, height).flatMap {
          case false if height >= swap.cltvExpiry =>
            // Like loopd, we never give up on an htlc that may still confirm: bitcoind's wallet and other nodes may
            // broadcast it again, and we must then refund it. It can't confirm anymore once it has been double-spent.
            val htlcTx = swap.progress.htlcTx_opt.get
            isDoubleSpent(htlcTx).map {
              case true if removeInvoice(swap) =>
                env.chain.rollback(htlcTx).failed.foreach(t => logger.warn(s"swap ${swap.swapHash}: cannot unlock htlc inputs: ${t.getMessage}"))
                fail(swap, States.FailedTimeout, "htlc was double-spent before it confirmed")
              case true => swap
              case false =>
                logger.warn(s"swap ${swap.swapHash}: our htlc ${htlcTx.txid} expired before it confirmed: we'll refund it if it confirms")
                swap
            }
          case _ => Future.successful(swap)
        }
    }
  }

  /** Whether our htlc transaction can't confirm anymore: one of its inputs was spent by another confirmed transaction. */
  private def isDoubleSpent(htlcTx: Transaction): Future[Boolean] =
    Future.sequence(htlcTx.txIn.map(_.outPoint).map { input =>
      for {
        parentConfirmations <- env.chain.getTxConfirmations(input.txid)
        spendable <- env.chain.isOutputSpendable(input)
      } yield parentConfirmations.exists(_ > 0) && !spendable
    }).flatMap {
      // Our htlc's inputs would also be spent if it just confirmed.
      case spent if spent.contains(true) => env.chain.getTxConfirmations(htlcTx.txid).map(!_.exists(_ > 0))
      case _ => Future.successful(false)
    }

  private def refund(swap: Swap, htlcOutput: HtlcOutput, htlcConfirmations: Int, height: BlockHeight): Future[Swap] = {
    for {
      htlcSpendable <- env.chain.isOutputSpendable(htlcOutput.outPoint)
      refunds <- Future.sequence(swap.progress.refundTxs.map(tx => env.chain.getTxConfirmations(tx.txid).map(c => tx -> c.getOrElse(0))))
      swap1 <- refunds.find(_._2 > 0) match {
        case Some((tx, confirmations)) if confirmations >= RefundMinDepth =>
          log(swap, s"refund ${tx.txid} confirmed (amount=${tx.txOut.head.amount})")
          Future.successful(update(swap, States.FailedTimeout, refunded(swap, tx)))
        case Some(_) => Future.successful(swap)
        case None if !htlcSpendable => htlcSpent(swap, htlcOutput, htlcConfirmations, height)
        case None if lastPublishHeight_opt.contains(height) => Future.successful(swap)
        case None if !removeInvoice(swap) => Future.successful(swap)
        case None => publishRefund(swap, htlcOutput, height)
      }
    } yield swap1
  }

  /** The htlc was spent by a transaction that isn't one of the refunds we kept: an older refund, or the server's sweep. */
  private def htlcSpent(swap: Swap, htlcOutput: HtlcOutput, htlcConfirmations: Int, height: BlockHeight): Future[Swap] =
    LoopChain.findSpendingTx(env.chain, htlcOutput.outPoint, height - htlcConfirmations + 1, height).map {
      case Some((tx, spentAt)) if tx.txOut.forall(_.publicKeyScript == swap.contract.refundScript) =>
        if ((height - spentAt).toInt + 1 >= RefundMinDepth) {
          log(swap, s"refund ${tx.txid} confirmed (amount=${tx.txOut.map(_.amount).sum})")
          update(swap, States.FailedTimeout, refunded(swap, tx))
        } else swap
      case Some(_) =>
        // Only the server can have spent it, with the preimage: it paid us and we'll see it at the next step.
        log(swap, "htlc was spent by the server")
        swap
      // Eclair's block height may lag behind bitcoind's: we'll find it at the next step.
      case None => swap
    }

  /** Our progress once this refund confirmed: it is our last refund (it may be one that we didn't keep). */
  private def refunded(swap: Swap, tx: Transaction): LoopInProgress =
    swap.progress.copy(refundTxs = (swap.progress.refundTxs.filterNot(_.txid == tx.txid) :+ tx).takeRight(MaxRefundTxs))

  private def publishRefund(swap: Swap, htlcOutput: HtlcOutput, height: BlockHeight): Future[Swap] = {
    val htlc = LoopIn.htlc(swap, env.keys)
    val contract = swap.contract
    val feerate = env.chain.feerate(RefundConfTarget)
    val previous_opt = swap.progress.refundTxs.lastOption
    val maxFee = contract.limits.maxMinerFee.min(htlcOutput.txOut.amount - LoopOutProcessor.MinSweepOutput)
    lastPublishHeight_opt = Some(height)
    Htlc.replacementFee(feerate, htlc.refundWeight(contract.refundScript), maxFee, previous_opt.map(p => htlcOutput.txOut.amount - p.txOut.map(_.amount).sum)) match {
      case None =>
        previous_opt match {
          case Some(tx) => env.chain.publish(tx).map(_ => swap).recover { case _ => swap }
          case None =>
            logger.warn(s"swap ${swap.swapHash}: cannot refund htlc: fee exceeds max miner fee ${contract.limits.maxMinerFee}")
            Future.successful(swap)
        }
      case Some(fee) =>
        val unsigned = Htlc.refundTx(htlc, htlcOutput.outPoint, htlcOutput.txOut.amount, contract.refundScript, fee)
        val tx = Htlc.signTimeout(htlc, unsigned, htlcOutput.txOut, env.keys.swapKey(swap.record.keyIndex))
        Transaction.correctlySpends(tx, Map(htlcOutput.outPoint -> htlcOutput.txOut), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
        val swap1 = update(swap, swap.state, swap.progress.copy(refundTxs = (swap.progress.refundTxs :+ tx).takeRight(MaxRefundTxs)))
        log(swap, s"publishing refund ${tx.txid} (fee=$fee feerate=$feerate)")
        env.chain.publish(tx).map(_ => swap1).recover {
          case t =>
            logger.warn(s"swap ${swap.swapHash}: cannot publish refund ${tx.txid}: ${t.getMessage}")
            swap1
        }
    }
  }

  // --- The server paid us ---

  private def settled(swap: Swap, height: BlockHeight): Future[Swap] = {
    for {
      swap1 <- pushKey(swap)
      htlcOutput_opt = LoopIn.htlcOutput(swap1, env.keys)
      htlcSwept <- htlcOutput_opt match {
        case Some(o) => env.chain.getTxConfirmations(o.outPoint.txid).flatMap {
          case Some(c) if c > 0 => env.chain.isOutputSpendable(o.outPoint).map(spendable => !spendable)
          case _ => rebroadcastHtlc(swap1, height).map(_ => false)
        }
        case None => Future.successful(false)
      }
    } yield {
      // Like loopd, we don't wait for the server's sweep after the htlc expired: we've been paid anyway.
      if (htlcSwept || height >= swap.cltvExpiry) update(swap1, States.Success, swap1.progress) else swap1
    }
  }

  /** Gives our MuSig2 internal key to the server, so that it can sweep the htlc more cheaply and privately. */
  private def pushKey(swap: Swap): Future[Swap] = {
    if (swap.progress.keyPushed) {
      Future.successful(swap)
    } else {
      env.server.pushKey(swap.swapHash, LoopIn.internalKey(swap, env.keys))
        .map { _ =>
          log(swap, "internal key pushed to the server")
          update(swap, swap.state, swap.progress.copy(keyPushed = true))
        }
        .recover {
          case t =>
            logger.warn(s"swap ${swap.swapHash}: cannot push internal key: ${t.getMessage}")
            swap
        }
    }
  }

}

object LoopInProcessor {
  /** Estimated weight of our htlc transaction (one p2wpkh input, the htlc and a p2wpkh change output). */
  val HtlcTxWeight = 610

  def htlcFeeEstimate(feerate: FeeratePerKw): Satoshi = Transactions.weight2fee(feerate, HtlcTxWeight)
}
