package io.github.rorp.sluice

import akka.actor.typed.Behavior
import fr.acinq.bitcoin.ScriptFlags
import fr.acinq.bitcoin.scalacompat.{ByteVector32, Musig2, OutPoint, Satoshi, SatoshiLong, Script, Transaction}
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import fr.acinq.eclair.payment.{Bolt11Invoice, PaymentEvent, PaymentFailed, PaymentSent}
import io.github.rorp.sluice.db.SwapRecord
import io.github.rorp.sluice.swapserverrpc.PaymentFailureReason
import fr.acinq.eclair.transactions.Transactions
import fr.acinq.eclair.{BlockHeight, MilliSatoshi, ShortChannelId, TimestampMilli, randomBytes32}
import grizzled.slf4j.Logging

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * A loop out swap (off-chain -> on-chain), ported from loopd's loopout.go:
 *  1. we pay the server's swap invoice (a hold invoice for our swap hash) and prepay invoice;
 *  2. the server locks the swap amount in an on-chain htlc that we can claim with the preimage;
 *  3. once the htlc is confirmed, we reveal the preimage and sweep the htlc, cooperatively with the server (MuSig2 key
 *     path) or alone (script path), bumping the fee until the sweep confirms;
 *  4. the server learns the preimage (from us or from our sweep) and settles our swap payment.
 *
 * We never reveal the preimage before the htlc is confirmed, or too close to its expiry: from then on, the server can
 * take our off-chain payment and we must sweep before the server can reclaim the htlc.
 */
object LoopOut {

  // @formatter:off
  object States {
    /** Paying the server and waiting for its htlc. */
    val Initiated = "INITIATED"
    /** We revealed the preimage and are sweeping the htlc. */
    val PreimageRevealed = "PREIMAGE_REVEALED"
    val Success = "SUCCESS"
    /** One of our payments failed before we revealed the preimage. */
    val FailedOffchainPayments = "FAILED_OFFCHAIN_PAYMENTS"
    /** The htlc wasn't confirmed early enough to safely reveal the preimage. */
    val FailedTimeout = "FAILED_TIMEOUT"
    /** The htlc doesn't pay the swap amount. */
    val FailedIncorrectHtlcAmount = "FAILED_INCORRECT_HTLC_AMOUNT"
    /** The htlc was spent by someone else (the server's timeout path) after we revealed the preimage. */
    val FailedSweepTimeout = "FAILED_SWEEP_TIMEOUT"
    /** The swap contract we persisted can't be used anymore. */
    val FailedInvalid = "FAILED_INVALID"
    /** The user abandoned the swap before we revealed the preimage. */
    val FailedAbandoned = "FAILED_ABANDONED"
  }
  // @formatter:on

  def isFinal(state: String): Boolean = state != States.Initiated && state != States.PreimageRevealed

  // Values from loopd (loopout.go).
  /** Min number of blocks before the htlc expiry to reveal the preimage. */
  val MinPreimageRevealDelta = 20
  /** When the htlc expires in less blocks than this, we sweep with an urgent confirmation target. */
  val SweepConfTargetDelta = 10
  val UrgentSweepConfTarget = 3
  /** After this many failed attempts at a cooperative sweep, we sweep through the script path. */
  val MaxMusig2Failures = 10
  /**
   * Confirmations of our sweep before we consider the swap done, and of the server's timeout transaction before we
   * consider it failed: a reorg could undo it until then, and let our sweep confirm instead.
   */
  val SweepMinDepth = 3
  /** We keep the last published sweeps, in case a replaced one confirms (we recognize older ones by their output). */
  val MaxSweepTxs = 25
  /** We raise the feerate of our sweep by this every block until it confirms, even if estimates don't change (loopd's sweep batcher). */
  val SweepFeerateStep: FeeratePerKw = FeeratePerKw(100 sat)
  /**
   * Max share of the swap amount that we pay in miner fees (loopd's MaxFeeToAmountRatio): it caps our default max
   * miner fee, and the fee of our sweep when the htlc is about to expire, when we may exceed the max miner fee.
   */
  val MaxFeeToAmountPercent = 20

  def maxFeeToAmount(amount: Satoshi): Satoshi = amount * MaxFeeToAmountPercent / 100

  case class Swap(record: SwapRecord, data: LoopOutData) extends SwapLike {
    val swapHash: ByteVector32 = record.swapHash
    val state: String = record.state
    val amount: Satoshi = record.amount
    val cltvExpiry: BlockHeight = record.cltvExpiry
    val contract: LoopOutContract = data.contract
    val progress: LoopOutProgress = data.progress
  }

  object Swap {
    def apply(record: SwapRecord): Swap = Swap(record, LoopOutData.decode(record.contract))
  }

  def apply(swap: Swap, env: SwapEnv): Behavior[SwapActor.Command] = SwapActor(swap, ec => new LoopOutProcessor(env)(ec))

}

/**
 * The loop out logic: each step looks at the chain and our payments, and moves the swap forward.
 *
 * @param feerateStep how much we raise the feerate of our sweep every block until it confirms.
 */
class LoopOutProcessor(env: SwapEnv, feerateStep: FeeratePerKw = LoopOut.SweepFeerateStep)(implicit ec: ExecutionContext) extends SwapProcessor[LoopOut.Swap] with Logging {

  import LoopOut._

  /** Our payments (eclair's ids) whose PaymentFailed event we've seen: their failure is final. */
  private val failedPayments = ConcurrentHashMap.newKeySet[UUID]()
  /**
   * The invoices that we paid since we started, with the id of our last payment once eclair gave it: eclair may still
   * be retrying it if its parts failed. We may pay an invoice again after a restart, when an earlier payment can't
   * complete: that payment's failure isn't the failure of the new one.
   */
  private val sentPayments = TrieMap.empty[ByteVector32, Option[UUID]]
  /** The last height at which we published or re-broadcast our sweep. */
  @volatile private var lastSweepHeight_opt: Option[BlockHeight] = None
  /** The feerate we targeted with our last sweep of this htlc output (we may pay more, to replace an earlier sweep). */
  @volatile private var sweepFeerate_opt: Option[(OutPoint, FeeratePerKw)] = None

  private def isOurPayment(swap: Swap, paymentHash: ByteVector32): Boolean = paymentHash == swap.swapHash || paymentHash == prepayHash(swap)

  override def onPaymentEvent(swap: Swap, event: PaymentEvent): Boolean = event match {
    case e: PaymentSent => isOurPayment(swap, e.paymentHash)
    case e: PaymentFailed if isOurPayment(swap, e.paymentHash) =>
      failedPayments.add(e.id)
      true
    case _ => false
  }

  override def reload(swap: Swap): Swap = env.db.getSwap(swap.swapHash).map(Swap(_)).getOrElse(swap)

  override def isFinal(swap: Swap): Boolean = LoopOut.isFinal(swap.state)

  private def prepayHash(swap: Swap): ByteVector32 = Bolt11Invoice.fromString(swap.contract.prepayInvoice).get.paymentHash

  private def log(swap: Swap, msg: String): Unit = logger.info(s"swap ${swap.swapHash}: $msg")

  override def step(swap: Swap): Future[Swap] = swap.state match {
    case States.Initiated => initiated(swap)
    case States.PreimageRevealed => sweeping(swap)
    case _ => Future.successful(swap)
  }

  private def update(swap: Swap, state: String, progress: LoopOutProgress): Swap = {
    if (state != swap.state || progress != swap.progress) {
      if (state != swap.state) log(swap, s"${swap.state} -> $state")
      val data = swap.data.copy(progress = progress)
      val now = TimestampMilli.now()
      env.db.updateSwap(swap.swapHash, state, LoopOutData.encode(data), now)
      swap.copy(record = swap.record.copy(state = state, contract = LoopOutData.encode(data), updatedAt = now), data = data)
    } else {
      swap
    }
  }

  private def fail(swap: Swap, state: String, reason: String): Swap = {
    logger.warn(s"swap ${swap.swapHash}: failed: $reason")
    update(swap, state, swap.progress)
  }

  // --- Paying the server and waiting for its htlc ---

  private def paymentStatus(paymentHash: ByteVector32): SwapPaymentStatus = {
    val failureFinal = sentPayments.get(paymentHash) match {
      // Nothing retries the payments that we sent before a restart.
      case None => true
      case Some(id_opt) => id_opt.exists(failedPayments.contains)
    }
    env.payments.status(paymentHash, failureFinal)
  }

  /** Pays the swap and prepay invoices, unless we already did (before a restart for example). */
  private def payInvoices(swap: Swap): Future[Unit] = {
    val swapInvoice = Bolt11Invoice.fromString(swap.contract.swapInvoice).get
    val prepayInvoice = Bolt11Invoice.fromString(swap.contract.prepayInvoice).get
    val limits = swap.contract.limits
    // eclair only records a payment in its db once it has found a route: until then, we see no payment for a payment
    // that is being sent, and we must not pay its invoice again.
    val toPay = Seq(
      (swapInvoice, limits.maxSwapRoutingFee),
      (prepayInvoice, limits.maxPrepayRoutingFee),
    ).filter { case (invoice, _) => !sentPayments.contains(invoice.paymentHash) && env.payments.status(invoice.paymentHash, failureFinal = true) == SwapPaymentStatus.NotSent }
    if (toPay.isEmpty) return Future.unit
    // None if we can't send payments now, otherwise the first hop to give eclair (if any): the channel may have been
    // announced since we created the swap, eclair then knows it by another id.
    val canSend: Future[Option[Option[ShortChannelId]]] = swap.contract.firstHop_opt match {
      case Some(firstHop) => env.payments.firstHopChannel(firstHop).map(_.collect { case c if c.canSend > MilliSatoshi(0) => Some(c.routerScid) })
      case None => env.payments.canSend().map(Option.when(_)(None))
    }
    canSend.flatMap {
      case None =>
        log(swap, s"waiting for ${swap.contract.firstHop_opt.map(c => s"our channel $c").getOrElse("our channels")} to be online before paying the server")
        Future.unit
      case Some(firstHop_opt) =>
        Future.sequence(toPay.map { case (invoice, maxFee) =>
          log(swap, s"paying ${invoice.amount_opt.get} to ${invoice.nodeId} (payment_hash=${invoice.paymentHash}, max_fee=$maxFee${firstHop_opt.map(c => s", first_hop=$c").getOrElse("")})")
          sentPayments.put(invoice.paymentHash, None)
          env.payments.pay(invoice, maxFee, firstHop_opt).andThen {
            case Success(id) => sentPayments.put(invoice.paymentHash, Some(id))
            // eclair rejected the payment before sending it (an expired invoice for example): we may try again.
            case Failure(_: IllegalArgumentException) => sentPayments.remove(invoice.paymentHash)
          }
        }).map(_ => ())
    }
  }

  private def initiated(swap: Swap): Future[Swap] = {
    val height = env.chain.currentHeight()
    payInvoices(swap).flatMap { _ =>
      val swapStatus = paymentStatus(swap.swapHash)
      val prepayStatus = paymentStatus(prepayHash(swap))
      if (swapStatus == SwapPaymentStatus.Failed || prepayStatus == SwapPaymentStatus.Failed) {
        val swap1 = fail(swap, States.FailedOffchainPayments, s"payment failed (swap=$swapStatus prepay=$prepayStatus)")
        cancelSwap(swap1, prepay = swapStatus != SwapPaymentStatus.Failed)
      } else {
        findHtlc(swap, height).flatMap { swap1 =>
          swap1.progress.htlc_opt match {
            case None if height >= swap.cltvExpiry - MinPreimageRevealDelta =>
              Future.successful(fail(swap1, States.FailedTimeout, s"htlc not found before height ${swap.cltvExpiry - MinPreimageRevealDelta}"))
            case None => Future.successful(swap1)
            case Some(htlcOutput) => checkHtlc(swap1, htlcOutput, height)
          }
        }
      }
    }
  }

  /** Tells the server why we gave up (it may help it route payments): failures are ignored. */
  private def cancelSwap(swap: Swap, prepay: Boolean): Future[Swap] = {
    val failure = env.payments.failureInfo(if (prepay) prepayHash(swap) else swap.swapHash)
    val paymentAddress = Bolt11Invoice.fromString(swap.contract.swapInvoice).get.paymentSecret
    // The server's node rejected our payment: it already cancelled the swap, and only accepts one cancellation per swap.
    if (failure.reason == PaymentFailureReason.LND_FAILURE_REASON_INCORRECT_PAYMENT_DETAILS) {
      log(swap, s"swap was cancelled by the server (prepay=$prepay)")
      return Future.successful(swap)
    }
    env.server.cancelLoopOutSwap(swap.swapHash, paymentAddress, prepay, failure)
      .map(_ => log(swap, s"swap cancelled on the server (prepay=$prepay reason=${failure.reason})"))
      .recover { case t => logger.warn(s"swap ${swap.swapHash}: cannot cancel swap on the server: ${t.getMessage}") }
      .map(_ => swap)
  }

  override def abandon(swap: Swap): Either[String, Swap] = swap.state match {
    // We haven't revealed the preimage: the server can't take our swap payment, which will fail back to us once its
    // htlc expires (we lose the prepay if the server already settled it).
    case States.Initiated => Right(fail(swap, States.FailedAbandoned, "abandoned by the user"))
    case States.PreimageRevealed => Left("the preimage has been revealed: the htlc must be swept, the swap can't be abandoned")
    case state => Left(s"swap is already final ($state)")
  }

  /** The htlc is published by the server: we don't know its txid, so we look for its script in new blocks. */
  private def findHtlc(swap: Swap, height: BlockHeight): Future[Swap] = {
    if (swap.progress.htlc_opt.nonEmpty) Future.successful(swap)
    else scanBlocks(swap, height).map(progress => update(swap, swap.state, progress))
  }

  /** Forgets the htlc and the blocks we scanned: we look for the htlc again from the start of the swap. */
  private def rescan(swap: Swap): LoopOutProgress =
    swap.progress.copy(scannedHeight = swap.contract.initiationHeight - 1, scannedBlockId_opt = None, htlc_opt = None)

  /**
   * Scans the blocks following the last one we scanned, until we find an htlc output. If they don't extend the blocks
   * that we scanned, those were reorganized out: we scan again from the start of the swap, since the htlc may now be in
   * a block that we didn't scan.
   *
   * @return our scanning progress, with the htlc that we found (if any).
   */
  private def scanBlocks(swap: Swap, height: BlockHeight): Future[LoopOutProgress] = {
    val htlcScript = htlc(swap).pubkeyScriptBytes

    def scan(progress: LoopOutProgress): Future[LoopOutProgress] = {
      val h = progress.scannedHeight + 1
      if (progress.htlc_opt.nonEmpty || h > height) Future.successful(progress)
      else env.chain.getBlock(h).flatMap {
        case block if progress.scannedBlockId_opt.exists(_ != block.parentId) =>
          log(swap, s"block ${progress.scannedBlockId_opt.get} at height ${progress.scannedHeight} was reorganized out, scanning again from height ${swap.contract.initiationHeight}")
          scan(rescan(swap))
        case block =>
          val htlc_opt = findOutput(block.txs, htlcScript)
          htlc_opt.foreach(o => log(swap, s"found htlc ${o.outPoint.txid}:${o.outPoint.index} (amount=${o.txOut.amount}) at height $h"))
          scan(progress.copy(scannedHeight = h, scannedBlockId_opt = Some(block.blockId), htlc_opt = htlc_opt))
      }
    }

    if (height < swap.progress.scannedHeight) {
      log(swap, s"our best chain (height=$height) is shorter than the blocks we scanned (height=${swap.progress.scannedHeight}), scanning again from height ${swap.contract.initiationHeight}")
      scan(rescan(swap))
    } else {
      scan(swap.progress.copy(htlc_opt = None))
    }
  }

  private def findOutput(txs: Seq[Transaction], script: scodec.bits.ByteVector): Option[HtlcOutput] = txs.view.flatMap { tx =>
    tx.txOut.zipWithIndex.collectFirst { case (o, i) if o.publicKeyScript == script => HtlcOutput(OutPoint(tx, i), o) }
  }.headOption

  private def checkHtlc(swap: Swap, htlcOutput: HtlcOutput, height: BlockHeight): Future[Swap] = {
    env.chain.getTxConfirmations(htlcOutput.outPoint.txid).flatMap {
      case None | Some(0) =>
        // The htlc was found in a block that has been reorganized out: we look for it again.
        log(swap, s"htlc ${htlcOutput.outPoint.txid} isn't confirmed anymore")
        Future.successful(update(swap, swap.state, rescan(swap)))
      case Some(_) if htlcOutput.txOut.amount < swap.amount =>
        Future.successful(fail(swap, States.FailedIncorrectHtlcAmount, s"htlc amount ${htlcOutput.txOut.amount} is below swap amount ${swap.amount}"))
      case Some(confirmations) if confirmations < swap.contract.htlcConfirmations => Future.successful(swap)
      case Some(_) if height >= swap.cltvExpiry - MinPreimageRevealDelta =>
        Future.successful(fail(swap, States.FailedTimeout, s"too late to reveal the preimage (height=$height expiry=${swap.cltvExpiry})"))
      case Some(_) if revealSweepFee(swap) > maxSweepFee(swap, htlcOutput, urgent = false) =>
        // Like loopd, we don't reveal the preimage if we can't sweep within our limits: the server could then take our
        // payment while our sweep doesn't confirm. We wait for feerates to drop, or we give up at the reveal deadline
        // and only lose the prepay.
        logger.warn(s"swap ${swap.swapHash}: not revealing the preimage: sweep fee ${revealSweepFee(swap)} exceeds max miner fee ${swap.contract.limits.maxMinerFee}")
        Future.successful(swap)
      case Some(_) =>
        // From now on we must sweep the htlc: we persist that before revealing anything.
        sweeping(update(swap, States.PreimageRevealed, swap.progress))
    }
  }

  /**
   * Pushing the preimage lets the server settle our payment earlier, and the server needs it to co-sign our cooperative
   * sweep (which doesn't reveal it on-chain). Like loopd, we push it at every step until the server has it: it took our
   * push, or it settled our payment. If the push fails we sweep anyway.
   */
  private def pushPreimage(swap: Swap): Future[Swap] = {
    lazy val settled = paymentStatus(swap.swapHash) match {
      case _: SwapPaymentStatus.Succeeded => true
      case _ => false
    }
    if (swap.progress.preimagePushed || settled) {
      Future.successful(swap)
    } else {
      // We persist that the server has it only once it does (a call may throw instead of failing its future).
      Future.unit.flatMap(_ => env.server.loopOutPushPreimage(swap.record.preimage))
        .map { _ =>
          log(swap, "preimage pushed to the server")
          update(swap, swap.state, swap.progress.copy(preimagePushed = true))
        }
        .recover {
          case t =>
            logger.warn(s"swap ${swap.swapHash}: cannot push preimage: ${t.getMessage}")
            swap
        }
    }
  }

  /** Fee of a script path sweep (heavier than a cooperative one) at our confirmation target. */
  private def revealSweepFee(swap: Swap): Satoshi =
    Transactions.weight2fee(env.chain.feerate(swap.contract.sweepConfTarget), htlc(swap).sweepWeight(swap.contract.sweepScript, keyPath = false))

  /**
   * The most we pay for a sweep. When the htlc is about to expire, a sweep that doesn't confirm loses the whole swap
   * amount: we then pay more than the max miner fee if needed, up to a share of the htlc amount (like loopd).
   */
  private def maxSweepFee(swap: Swap, htlcOutput: HtlcOutput, urgent: Boolean): Satoshi = {
    val htlcAmount = htlcOutput.txOut.amount
    val maxFee = if (urgent) swap.contract.limits.maxMinerFee.max(maxFeeToAmount(htlcAmount)) else swap.contract.limits.maxMinerFee
    maxFee.min(htlcAmount - LoopOutProcessor.MinSweepOutput)
  }

  // --- Sweeping the htlc ---

  def htlc(swap: Swap): Htlc = {
    val receiverKey = env.keys.swapKey(swap.record.keyIndex).publicKey
    val serverKey = swap.contract.serverKey
    Htlc(swap.swapHash, swap.cltvExpiry, serverKey, receiverKey, serverKey, receiverKey)
  }

  private def sweeping(swap0: Swap): Future[Swap] = {
    val height = env.chain.currentHeight()
    val htlcOutput = swap0.progress.htlc_opt.get
    for {
      swap <- pushPreimage(swap0)
      // We check whether the htlc is spent before looking at our sweeps: if one of them confirms in-between, we'll
      // simply see it at the next step.
      htlcSpendable <- env.chain.isOutputSpendable(htlcOutput.outPoint)
      htlcConfirmations <- env.chain.getTxConfirmations(htlcOutput.outPoint.txid)
      sweeps <- Future.sequence(swap.progress.sweepTxs.map(tx => env.chain.getTxConfirmations(tx.txid).map(c => tx -> c.getOrElse(0))))
      swap1 <- sweeps.find(_._2 > 0) match {
        case Some((tx, confirmations)) if confirmations >= SweepMinDepth =>
          log(swap, s"sweep ${tx.txid} confirmed (amount=${tx.txOut.head.amount})")
          Future.successful(update(swap, States.Success, swept(swap, tx)))
        case Some(_) => Future.successful(swap)
        case None if !htlcSpendable && htlcConfirmations.exists(_ > 0) => htlcSpent(swap, htlcOutput, htlcConfirmations.get, height)
        case None if !htlcConfirmations.exists(_ > 0) => relocateHtlc(swap, htlcOutput, height)
        case None if lastSweepHeight_opt.contains(height) => Future.successful(swap)
        case None => publishSweep(swap, htlcOutput, height)
      }
    } yield swap1
  }

  /** Our progress once this sweep confirmed: it is our last sweep (it may be one that we didn't keep). */
  private def swept(swap: Swap, tx: Transaction): LoopOutProgress =
    swap.progress.copy(sweepTxs = (swap.progress.sweepTxs.filterNot(_.txid == tx.txid) :+ tx).takeRight(MaxSweepTxs))

  /**
   * The htlc was spent by a transaction that isn't one of the sweeps we kept: one of our older sweeps, or the server
   * reclaiming the htlc after its expiry. We wait until it is deeply confirmed: a reorg may still undo it.
   */
  private def htlcSpent(swap: Swap, htlcOutput: HtlcOutput, htlcConfirmations: Int, height: BlockHeight): Future[Swap] =
    LoopChain.findSpendingTx(env.chain, htlcOutput.outPoint, height - htlcConfirmations + 1, height).map {
      case Some((tx, spentAt)) if (height - spentAt).toInt + 1 >= SweepMinDepth =>
        if (tx.txOut.forall(_.publicKeyScript == swap.contract.sweepScript)) {
          log(swap, s"sweep ${tx.txid} confirmed (amount=${tx.txOut.map(_.amount).sum})")
          update(swap, States.Success, swept(swap, tx))
        } else {
          fail(swap, States.FailedSweepTimeout, s"htlc was spent by ${tx.txid}")
        }
      case Some((tx, _)) =>
        log(swap, s"htlc was spent by ${tx.txid}, waiting for it to be deeply confirmed")
        swap
      // Eclair's block height may lag behind bitcoind's: we'll find it at the next step.
      case None => swap
    }

  /**
   * The htlc was reorganized out: it may be back in our mempool, or the server may have double-spent it, possibly with
   * another htlc for our swap. The server knows the preimage: we must sweep whichever htlc confirms, so we look for one
   * in new blocks (and in all blocks since the start of the swap if the blocks we scanned were reorganized out).
   */
  private def relocateHtlc(swap: Swap, htlcOutput: HtlcOutput, height: BlockHeight): Future[Swap] =
    scanBlocks(swap, height).flatMap { progress =>
      progress.htlc_opt match {
        case Some(found) =>
          if (found.outPoint != htlcOutput.outPoint) log(swap, s"htlc ${htlcOutput.outPoint.txid} was replaced by ${found.outPoint.txid}:${found.outPoint.index} (amount=${found.txOut.amount})")
          publishSweep(update(swap, swap.state, progress), found, height)
        case None =>
          logger.warn(s"swap ${swap.swapHash}: htlc ${htlcOutput.outPoint.txid} isn't confirmed anymore")
          // We keep sweeping the htlc we know, which may still be in our mempool.
          val swap1 = update(swap, swap.state, progress.copy(htlc_opt = Some(htlcOutput)))
          if (lastSweepHeight_opt.contains(height)) Future.successful(swap1) else publishSweep(swap1, htlcOutput, height)
      }
    }

  private def publishSweep(swap: Swap, htlcOutput: HtlcOutput, height: BlockHeight): Future[Swap] = {
    val contract = swap.contract
    val urgent = height >= swap.cltvExpiry - SweepConfTargetDelta
    val estimate = env.chain.feerate(if (urgent) UrgentSweepConfTarget else contract.sweepConfTarget)
    val keyPath = swap.progress.musig2Failures < MaxMusig2Failures
    // Our sweep to replace, if the htlc didn't change since we published it.
    val previous_opt = swap.progress.sweepTxs.findLast(_.txIn.exists(_.outPoint == htlcOutput.outPoint))
    // Our previous sweep didn't confirm: we raise its feerate (after a restart, we start from the feerate it pays).
    val previousFeerate_opt = sweepFeerate_opt.collect { case (outPoint, feerate) if outPoint == htlcOutput.outPoint => feerate }
      .orElse(previous_opt.map(tx => Transactions.fee2rate(htlcOutput.txOut.amount - tx.txOut.map(_.amount).sum, tx.weight())))
    val feerate = previousFeerate_opt.map(f => (f + feerateStep).max(estimate)).getOrElse(estimate)
    sweepFeerate_opt = Some(htlcOutput.outPoint -> feerate)
    sweepFee(swap, htlcOutput, feerate, keyPath, urgent, previous_opt) match {
      case None =>
        // The current sweep pays enough (or as much as we accept): we only make sure it stays in the mempool.
        lastSweepHeight_opt = Some(height)
        previous_opt match {
          case Some(tx) => env.chain.publish(tx).map(_ => swap).recover { case _ => swap }
          case None =>
            logger.warn(s"swap ${swap.swapHash}: cannot sweep htlc: fee exceeds max miner fee ${maxSweepFee(swap, htlcOutput, urgent)}")
            Future.successful(swap)
        }
      case Some(fee) =>
        val signed = if (keyPath) {
          cooperativeSweep(swap, htlcOutput, fee).map(tx => (tx, swap.progress.musig2Failures)).recover {
            case t =>
              logger.warn(s"swap ${swap.swapHash}: cooperative sweep failed: ${t.getMessage}")
              val scriptPathFee = sweepFee(swap, htlcOutput, feerate, keyPath = false, urgent, previous_opt).getOrElse(fee)
              (scriptPathSweep(swap, htlcOutput, scriptPathFee), swap.progress.musig2Failures + 1)
          }
        } else {
          Future.successful((scriptPathSweep(swap, htlcOutput, fee), swap.progress.musig2Failures))
        }
        signed.flatMap { case (tx, musig2Failures) =>
          Transaction.correctlySpends(tx, Map(htlcOutput.outPoint -> htlcOutput.txOut), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
          // We persist the sweep before publishing it, so that we always watch it.
          val swap1 = update(swap, swap.state, swap.progress.copy(sweepTxs = (swap.progress.sweepTxs :+ tx).takeRight(MaxSweepTxs), musig2Failures = musig2Failures))
          lastSweepHeight_opt = Some(height)
          log(swap, s"publishing sweep ${tx.txid} (key_path=${tx.txIn.head.witness.stack.size == 1} fee=${htlcOutput.txOut.amount - tx.txOut.map(_.amount).sum} feerate=$feerate urgent=$urgent)")
          env.chain.publish(tx).map(_ => swap1).recover {
            case t =>
              logger.warn(s"swap ${swap.swapHash}: cannot publish sweep ${tx.txid}: ${t.getMessage}")
              swap1
          }
        }
    }
  }

  /**
   * Fee of a new sweep at the given feerate, or None if we should keep the previous one: replacing it requires paying
   * more than it does plus the relay fee of the new transaction (BIP 125), and we never exceed [[maxSweepFee]].
   */
  private def sweepFee(swap: Swap, htlcOutput: HtlcOutput, feerate: FeeratePerKw, keyPath: Boolean, urgent: Boolean, previous_opt: Option[Transaction]): Option[Satoshi] =
    Htlc.replacementFee(feerate, htlc(swap).sweepWeight(swap.contract.sweepScript, keyPath), maxSweepFee(swap, htlcOutput, urgent), previous_opt.map(p => htlcOutput.txOut.amount - p.txOut.map(_.amount).sum))

  private def scriptPathSweep(swap: Swap, htlcOutput: HtlcOutput, fee: Satoshi): Transaction = {
    val unsigned = Htlc.sweepTx(htlcOutput.outPoint, htlcOutput.txOut.amount, swap.contract.sweepScript, fee, keyPath = false)
    Htlc.signSuccess(htlc(swap), unsigned, htlcOutput.txOut, env.keys.swapKey(swap.record.keyIndex), swap.record.preimage)
  }

  /** Key path sweep: we create a MuSig2 signature with the server, which reveals nothing about the swap on-chain. */
  private def cooperativeSweep(swap: Swap, htlcOutput: HtlcOutput, fee: Satoshi): Future[Transaction] = {
    val htlc = this.htlc(swap)
    val ourKey = env.keys.swapKey(swap.record.keyIndex)
    val unsigned = Htlc.sweepTx(htlcOutput.outPoint, htlcOutput.txOut.amount, swap.contract.sweepScript, fee, keyPath = true)
    val spent = Seq(htlcOutput.txOut)
    val ourNonce = Musig2.generateNonce(randomBytes32(), Left(ourKey), htlc.internalKeys, None, None)
    val paymentAddress = Bolt11Invoice.fromString(swap.contract.swapInvoice).get.paymentSecret
    env.server.muSig2SignSweep(swap.swapHash, paymentAddress, ourNonce.publicNonce, unsigned, Map(htlcOutput.outPoint -> htlcOutput.txOut)).flatMap { response =>
      val nonces = Seq(ourNonce.publicNonce, response.serverNonce)
      val result = for {
        _ <- Either.cond(Musig2.verifyTaprootSignature(response.partialSig, response.serverNonce, swap.contract.serverKey, unsigned, 0, spent, htlc.internalKeys, nonces, Some(htlc.scriptTree)), (), new IllegalArgumentException("invalid server partial signature"))
        ourSig <- Musig2.signTaprootInput(ourKey, unsigned, 0, spent, htlc.internalKeys, ourNonce.secretNonce, nonces, Some(htlc.scriptTree))
        sig <- Musig2.aggregateTaprootSignatures(Seq(ourSig, response.partialSig), unsigned, 0, spent, htlc.internalKeys, nonces, Some(htlc.scriptTree))
      } yield unsigned.updateWitness(0, Script.witnessKeyPathPay2tr(sig))
      Future.fromTry(result.toTry)
    }
  }

}

object LoopOutProcessor {
  /** We don't sweep if we'd get less than this (P2TR dust limit). */
  val MinSweepOutput: Satoshi = 330.sat
}
