package io.github.rorp.sluice

import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.adapter.{ClassicActorRefOps, ClassicSchedulerOps}
import akka.util.Timeout
import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.bitcoin.scalacompat.{BlockHash, BlockId, ByteVector32, KotlinUtils, OutPoint, Satoshi, Script, Transaction, TxId}
import fr.acinq.eclair.blockchain.bitcoind.rpc.BitcoinCoreClient
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import fr.acinq.eclair.channel.{ChannelDataWithCommitments, Commitments, DATA_NORMAL, NORMAL, Origin, PersistentChannelData, Upstream}
import fr.acinq.eclair.db.{FailureSummary, FailureType, IncomingPaymentStatus, OutgoingPayment, OutgoingPaymentStatus}
import io.github.rorp.sluice.swapserverrpc.PaymentFailureReason
import fr.acinq.eclair.router.Router
import fr.acinq.eclair.wire.protocol.{IncorrectOrUnknownPaymentDetails, UpdateAddHtlc, UpdateMessage}
import akka.pattern.ask
import io.github.rorp.sluice.db.LoopDb
import fr.acinq.eclair.transactions.DirectedHtlc
import fr.acinq.eclair.payment.Bolt11Invoice.ExtraHop
import fr.acinq.eclair.payment.receive.MultiPartHandler.ReceiveStandardPayment
import fr.acinq.eclair.payment.{Bolt11Invoice, LocalFailure, PaymentFailed}
import fr.acinq.eclair.{BlockHeight, EclairImpl, Kit, MilliSatoshi, MilliSatoshiLong, ShortChannelId, TimestampMilli}
import scodec.bits.ByteVector

import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** What swaps need to run. */
case class SwapEnv(server: SwapServerClient, chain: LoopChain, payments: LoopPayments, db: LoopDb, keys: LoopKeys, chainHash: BlockHash)

/** A block of our best chain: its parent tells us whether it extends the blocks we already looked at. */
case class ChainBlock(blockId: BlockId, parentId: BlockId, txs: Seq[Transaction])

/** On-chain operations used by swaps. */
trait LoopChain {
  def currentHeight(): BlockHeight

  /** The block at this height in our best chain. */
  def getBlock(height: BlockHeight): Future[ChainBlock]

  /** None if the transaction is unknown (neither confirmed nor in our mempool), Some(0) if it is in the mempool. */
  def getTxConfirmations(txId: TxId): Future[Option[Int]]

  /** Whether the output is in the confirmed utxo set. */
  def isOutputSpendable(outPoint: OutPoint): Future[Boolean]

  def publish(tx: Transaction): Future[TxId]

  def feerate(confTarget: Int): FeeratePerKw

  /** A new script from eclair's on-chain wallet. */
  def newSweepScript(): Future[ByteVector]

  /**
   * Creates and signs (but doesn't publish) a transaction paying amount to script from eclair's on-chain wallet.
   *
   * @return the transaction and its fee.
   */
  def fundOutput(script: ByteVector, amount: Satoshi, feerate: FeeratePerKw, maxFee: Satoshi): Future[(Transaction, Satoshi)]

  /** Releases the wallet inputs of a transaction that we didn't publish. */
  def rollback(tx: Transaction): Future[Unit]
}

sealed trait IncomingInvoiceStatus

object IncomingInvoiceStatus {
  /** The invoice doesn't exist (anymore). */
  case object Unknown extends IncomingInvoiceStatus

  case object Pending extends IncomingInvoiceStatus

  case class Received(amount: MilliSatoshi) extends IncomingInvoiceStatus
}

/**
 * Why an outgoing payment failed, as reported to the server when cancelling a loop out (like loopd).
 *
 * @param remainingHops for each failed attempt, the distance between the failing node and the destination.
 */
case class PaymentFailureInfo(reason: PaymentFailureReason, remainingHops: Seq[Long])

sealed trait SwapPaymentStatus

object SwapPaymentStatus {
  case object NotSent extends SwapPaymentStatus

  case object Pending extends SwapPaymentStatus

  case class Succeeded(preimage: ByteVector32, amount: MilliSatoshi, fees: MilliSatoshi) extends SwapPaymentStatus

  case object Failed extends SwapPaymentStatus
}

/**
 * One of our open channels, that we can pay through with eclair's firstHop.
 *
 * @param routerScid the id of this channel that eclair's firstHop must use: its real scid if it is announced, its local
 *                   alias otherwise (eclair's router doesn't know its other ids).
 * @param canSend    how much we can send through it now in htlcs that stay pending together (0 msat while it is offline):
 *                   our swap payments stay pending until we reveal the preimage, so they must fit in its balance and
 *                   in the value of htlcs in flight that our peer accepts.
 */
case class FirstHopChannel(routerScid: ShortChannelId, canSend: MilliSatoshi)

/** Off-chain payments made by swaps. */
trait LoopPayments {
  /** @param firstHop_opt only send the payment through this channel of ours (its [[FirstHopChannel.routerScid]]). */
  def pay(invoice: Bolt11Invoice, maxFee: Satoshi, firstHop_opt: Option[ShortChannelId]): Future[UUID]

  /**
   * Status of our payment(s) for this payment hash.
   *
   * @param failureFinal whether a payment whose parts all failed is final: eclair may still be retrying it, unless we
   *                     saw its PaymentFailed event or eclair was restarted since.
   */
  def status(paymentHash: ByteVector32, failureFinal: Boolean): SwapPaymentStatus

  /** Why our payment(s) for this payment hash failed. */
  def failureInfo(paymentHash: ByteVector32): PaymentFailureInfo

  /**
   * eclair forgets the htlcs it sent but didn't commit before stopping, but their payments stay pending in its db
   * forever. We fail them, and ignore them afterwards: their invoices can be paid again. Only call this at startup,
   * before sending payments: a payment that is being sent doesn't have committed htlcs yet.
   *
   * @return the payment hashes that had such payments.
   */
  def failOrphanedPayments(paymentHashes: Set[ByteVector32]): Seq[ByteVector32]

  /** Whether one of our channels can send payments now: payments fail right away otherwise (after a restart, before our peers reconnect for example). */
  def canSend(): Future[Boolean]

  /** Our open channel with this short channel id (its local alias or one of its real scids), if any. */
  def firstHopChannel(scid: ShortChannelId): Future[Option[FirstHopChannel]]

  /**
   * Routing hints for our private channels, like loopd's SelectHopHints: the server can't find a route to us without
   * them if we don't have public channels.
   *
   * @param lastHop_opt only use our channels with this peer.
   */
  def routeHints(lastHop_opt: Option[PublicKey]): Future[List[List[ExtraHop]]]

  /** Creates an invoice that we will accept, with our own preimage. */
  def createInvoice(preimage: ByteVector32, amount: MilliSatoshi, description: String, expiry: FiniteDuration, routeHints: List[List[ExtraHop]]): Future[Bolt11Invoice]

  /** An invoice signed with our node key that we don't register: payments to it fail when they reach our node. */
  def unregisteredInvoice(paymentHash: ByteVector32, amount: MilliSatoshi, description: String, routeHints: List[List[ExtraHop]] = Nil): Bolt11Invoice

  def incomingStatus(paymentHash: ByteVector32): IncomingInvoiceStatus

  /** Deletes an invoice so that we don't accept payments anymore: fails if it has already been paid. */
  def removeInvoice(paymentHash: ByteVector32): Try[Unit]
}

class BitcoinCoreLoopChain(kit: Kit, bitcoin: BitcoinCoreClient)(implicit ec: ExecutionContext) extends LoopChain {

  override def currentHeight(): BlockHeight = kit.nodeParams.currentBlockHeight

  override def getBlock(height: BlockHeight): Future[ChainBlock] = for {
    blockId <- bitcoin.getBlockId(height.toInt)
    block <- bitcoin.getBlock(blockId)
  } yield ChainBlock(blockId, BlockId(KotlinUtils.kmp2scala(block.header.hashPreviousBlock)), block.tx.asScala.toSeq.map(KotlinUtils.kmp2scala))

  override def getTxConfirmations(txId: TxId): Future[Option[Int]] = bitcoin.getTxConfirmations(txId)

  override def isOutputSpendable(outPoint: OutPoint): Future[Boolean] = bitcoin.isTransactionOutputSpendable(outPoint.txid, outPoint.index.toInt, includeMempool = false)

  override def publish(tx: Transaction): Future[TxId] = bitcoin.publishTransaction(tx)

  override def feerate(confTarget: Int): FeeratePerKw = LoopChain.feerateForTarget(kit.nodeParams.currentBitcoinCoreFeerates, confTarget)

  override def newSweepScript(): Future[ByteVector] = bitcoin.getReceivePublicKeyScript().map(Script.write)

  override def fundOutput(script: ByteVector, amount: Satoshi, feerate: FeeratePerKw, maxFee: Satoshi): Future[(Transaction, Satoshi)] =
    bitcoin.makeFundingTx(script, amount, feerate, Some(maxFee)).map(r => (r.fundingTx, r.fee))

  override def rollback(tx: Transaction): Future[Unit] = bitcoin.rollback(tx).map(_ => ())

}

object LoopChain {

  /**
   * The transaction of our best chain that spends this output, with its height: we look for it in blocks from `to` down
   * to `from`, since we usually look for a recent spend.
   */
  def findSpendingTx(chain: LoopChain, outPoint: OutPoint, from: BlockHeight, to: BlockHeight)(implicit ec: ExecutionContext): Future[Option[(Transaction, BlockHeight)]] =
    if (to < from) Future.successful(None)
    else chain.getBlock(to).flatMap { block =>
      block.txs.find(_.txIn.exists(_.outPoint == outPoint)) match {
        case Some(tx) => Future.successful(Some((tx, to)))
        case None => findSpendingTx(chain, outPoint, from, to - 1)
      }
    }

  /** eclair only tracks a few targets: fastest = 1 block, fast = 2 blocks, medium = 12 blocks, slow = 1008 blocks. */
  def feerateForTarget(feerates: fr.acinq.eclair.blockchain.fee.FeeratesPerKw, confTarget: Int): FeeratePerKw = {
    if (confTarget <= 1) feerates.fastest
    else if (confTarget <= 3) feerates.fast
    else if (confTarget <= 12) feerates.medium
    else feerates.slow
  }

}

class EclairLoopPayments(kit: Kit, paymentTimeout: FiniteDuration = 1.minute)(implicit ec: ExecutionContext) extends LoopPayments {

  private val eclair = new EclairImpl(kit)

  override def pay(invoice: Bolt11Invoice, maxFee: Satoshi, firstHop_opt: Option[ShortChannelId]): Future[UUID] = {
    implicit val timeout: Timeout = Timeout(paymentTimeout)
    // The proportional limit is 0 so that the flat limit applies (eclair uses the max of both).
    eclair.send(Some(EclairLoopPayments.ExternalId), invoice.amount_opt.get, invoice, maxFeeFlat_opt = Some(maxFee), maxFeePct_opt = Some(0), firstHop_opt = firstHop_opt)
  }

  override def status(paymentHash: ByteVector32, failureFinal: Boolean): SwapPaymentStatus =
    EclairLoopPayments.status(kit.nodeParams.db.payments.listOutgoingPayments(paymentHash), failureFinal)

  override def failureInfo(paymentHash: ByteVector32): PaymentFailureInfo =
    EclairLoopPayments.failureInfo(kit.nodeParams.db.payments.listOutgoingPayments(paymentHash))

  override def canSend(): Future[Boolean] = {
    implicit val timeout: Timeout = Timeout(paymentTimeout)
    // After a restart, channels keep their last (enabled) channel_update while they're offline: usable balances
    // include them, we must check their state.
    for {
      channels <- eclair.channelsInfo(None)
      balances <- eclair.usableBalances()
    } yield channels.exists(_.state == NORMAL) && balances.exists(_.canSend > MilliSatoshi(0))
  }

  override def firstHopChannel(scid: ShortChannelId): Future[Option[FirstHopChannel]] = {
    implicit val timeout: Timeout = Timeout(paymentTimeout)
    // An offline channel is still in DATA_NORMAL (in the OFFLINE state); a channel in another state can't send payments.
    eclair.channelsInfo(None).map(_.map(c => (c.state, c.data)).collectFirst {
      case (state, d: DATA_NORMAL) if (d.aliases.localAlias +: d.commitments.active.flatMap(_.shortChannelId_opt)).exists(_.toLong == scid.toLong) =>
        // Like eclair's router: an announced channel is a public channel, known by the scid of its announcement.
        val routerScid = d.lastAnnouncement_opt.map(_.shortChannelId).getOrElse(d.aliases.localAlias)
        FirstHopChannel(routerScid, if (state == NORMAL) EclairLoopPayments.canSendPending(d.commitments) else 0.msat)
    })
  }

  override def failOrphanedPayments(paymentHashes: Set[ByteVector32]): Seq[ByteVector32] = {
    val db = kit.nodeParams.db
    val committed = EclairLoopPayments.committedPaymentIds(db.channels.listLocalChannels())
    paymentHashes.toSeq.flatMap(h => EclairLoopPayments.orphanedPayments(db.payments.listOutgoingPayments(h), committed)).flatMap { p =>
      // This fails if the payment completed in the meantime.
      Try(db.payments.updateOutgoingPayment(PaymentFailed(p.id, p.paymentHash, Seq(LocalFailure(p.amount, Nil, EclairLoopPayments.OrphanedPayment)), p.createdAt))).toOption.map(_ => p.paymentHash)
    }.distinct
  }

  override def routeHints(lastHop_opt: Option[PublicKey]): Future[List[List[ExtraHop]]] = {
    implicit val timeout: Timeout = Timeout(paymentTimeout)
    (kit.router ? Router.GetRouterData).mapTo[Router.Data].map { d =>
      val channels = d.privateChannels.values.filter(c => lastHop_opt.forall(_ == c.remoteNodeId))
      channels.flatMap(_.toIncomingExtraHop).take(EclairLoopPayments.MaxRouteHints).map(_ :: Nil).toList
    }
  }

  override def createInvoice(preimage: ByteVector32, amount: MilliSatoshi, description: String, expiry: FiniteDuration, routeHints: List[List[ExtraHop]]): Future[Bolt11Invoice] = {
    implicit val timeout: Timeout = Timeout(paymentTimeout)
    implicit val scheduler: Scheduler = kit.system.scheduler.toTyped
    kit.paymentHandler.toTyped.ask[Bolt11Invoice](ref => ReceiveStandardPayment(ref, Some(amount), Left(description), Some(expiry.toSeconds), routeHints, None, Some(preimage)))
  }

  override def unregisteredInvoice(paymentHash: ByteVector32, amount: MilliSatoshi, description: String, routeHints: List[List[ExtraHop]]): Bolt11Invoice = {
    val nodeParams = kit.nodeParams
    Bolt11Invoice(nodeParams.chainHash, Some(amount), paymentHash, nodeParams.privateKey, Left(description), nodeParams.channelConf.minFinalExpiryDelta, expirySeconds = Some(3600), extraHops = routeHints, features = nodeParams.features.bolt11Features())
  }

  override def incomingStatus(paymentHash: ByteVector32): IncomingInvoiceStatus = kit.nodeParams.db.payments.getIncomingPayment(paymentHash).map(_.status) match {
    case Some(IncomingPaymentStatus.Received(amount, _)) => IncomingInvoiceStatus.Received(amount)
    case Some(_) => IncomingInvoiceStatus.Pending
    case None => IncomingInvoiceStatus.Unknown
  }

  override def removeInvoice(paymentHash: ByteVector32): Try[Unit] = kit.nodeParams.db.payments.removeIncomingPayment(paymentHash)

}

object EclairLoopPayments {

  /** Labels our swap payments in eclair's payments db (payments made before the plugin was renamed have eclair-loop-swap). */
  val ExternalId = "sluice-swap"

  /** Like lnd's SelectHopHints. */
  val MaxRouteHints = 20

  /** Failing nodes further than this from the destination are reported as "far" (loopd's loopInternalHops). */
  val LoopInternalHops = 2

  private val IncorrectOrUnknownPaymentDetailsMessage = IncorrectOrUnknownPaymentDetails(MilliSatoshi(0), BlockHeight(0)).message

  /** The failure we record for payments whose htlcs eclair forgot (see [[LoopPayments.failOrphanedPayments]]). */
  object OrphanedPayment extends RuntimeException("eclair stopped before the htlc was committed")

  val orphanedFailureSummary: FailureSummary = FailureSummary(FailureType.LOCAL, OrphanedPayment.getMessage, Nil, None)

  /** Ids of our payment parts that have a signed htlc in one of our channels. */
  def committedPaymentIds(channels: Seq[PersistentChannelData]): Set[UUID] = channels.collect {
    case c: ChannelDataWithCommitments => committedPaymentIds(c.commitments.originChannels, c.commitments.changes.localChanges.proposed)
  }.flatten.toSet

  /**
   * eclair stores the origin of an htlc as soon as it adds it, before signing it. If it stops in between, it drops the
   * htlc when the channel reconnects, but doesn't fail its payment (it only does that when it disconnects while
   * running): the payment stays pending forever, with an origin in the channel.
   *
   * @param origins  the origins of a channel's outgoing htlcs, by htlc id.
   * @param proposed the channel's local changes that aren't signed yet.
   */
  def committedPaymentIds(origins: Map[Long, Origin], proposed: Seq[UpdateMessage]): Set[UUID] = {
    val unsigned = proposed.collect { case add: UpdateAddHtlc => add.id }.toSet
    origins.collect { case (htlcId, origin) if !unsigned.contains(htlcId) => origin.upstream }.collect { case Upstream.Local(id) => id }.toSet
  }

  /** Pending payment parts without htlcs: eclair won't complete them, nor their payment (only true at startup). */
  def orphanedPayments(payments: Seq[OutgoingPayment], committedPaymentIds: Set[UUID]): Seq[OutgoingPayment] =
    payments.filter(p => p.status == OutgoingPaymentStatus.Pending && !committedPaymentIds.contains(p.id))

  private def isOrphaned(p: OutgoingPayment): Boolean = p.status match {
    case f: OutgoingPaymentStatus.Failed => f.failures == Seq(orphanedFailureSummary)
    case _ => false
  }

  /**
   * The parts that count: we ignore the failed parts of payments that had orphaned parts. Such a payment can't complete:
   * the recipient fails the parts it received once it gives up waiting for the others (after 2 minutes with lnd). Once
   * none of its parts is pending, the invoice can be paid again.
   */
  private def counted(payments: Seq[OutgoingPayment]): Seq[OutgoingPayment] = {
    val interrupted = payments.filter(isOrphaned).map(_.parentId).toSet
    payments.filterNot(p => interrupted.contains(p.parentId) && p.status.isInstanceOf[OutgoingPaymentStatus.Failed])
  }

  def failureInfo(payments: Seq[OutgoingPayment]): PaymentFailureInfo = {
    val failures = counted(payments).map(_.status).collect { case f: OutgoingPaymentStatus.Failed => f }.sortBy(_.completedAt).flatMap(_.failures)
    val reason = failures.lastOption match {
      case None => PaymentFailureReason.LND_FAILURE_REASON_ERROR
      // We couldn't find a route within our limits (fees, cltv expiry).
      case Some(f) if f.failureType == FailureType.LOCAL && Seq("route not found", "expiry too big").exists(f.failureMessage.toLowerCase.contains) => PaymentFailureReason.LND_FAILURE_REASON_NO_ROUTE
      case Some(f) if f.failureType == FailureType.LOCAL && f.failureMessage.toLowerCase.contains("balance too low") => PaymentFailureReason.LND_FAILURE_REASON_INSUFFICIENT_BALANCE
      // eclair stores the failure message's description (see FailureSummary.apply), not its name.
      case Some(f) if f.failureType == FailureType.REMOTE && f.failureMessage == IncorrectOrUnknownPaymentDetailsMessage => PaymentFailureReason.LND_FAILURE_REASON_INCORRECT_PAYMENT_DETAILS
      case Some(_) => PaymentFailureReason.LND_FAILURE_REASON_ERROR
    }
    val remainingHops = failures.collect {
      case f if f.failedRoute.nonEmpty && f.failedNode.nonEmpty =>
        val nodes = f.failedRoute.head.nodeId +: f.failedRoute.map(_.nextNodeId)
        nodes.indexOf(f.failedNode.get) match {
          case -1 => None
          case i =>
            val distance = (f.failedRoute.size - i).toLong
            Some(if (distance > LoopInternalHops) 0xffffffffL else distance)
        }
    }.flatten
    PaymentFailureInfo(reason, remainingHops)
  }

  /** How much we can send in new htlcs that stay pending together: within our balance and the max value in flight. */
  def canSendPending(commitments: Commitments): MilliSatoshi = {
    val inFlight = commitments.latest.localCommit.spec.htlcs.collect(DirectedHtlc.outgoing).toSeq.map(_.amountMsat).sum
    commitments.availableBalanceForSend.min((commitments.maxHtlcValueInFlight - inFlight).max(0.msat))
  }

  /** eclair retries failed parts quickly: if the last failure is recent, the payment may still be in progress. */
  val RetryGracePeriod: FiniteDuration = 1.minute

  def status(allPayments: Seq[OutgoingPayment], failureFinal: Boolean, now: TimestampMilli = TimestampMilli.now()): SwapPaymentStatus = {
    val payments = counted(allPayments)
    val succeeded = payments.flatMap(p => p.status match {
      case s: OutgoingPaymentStatus.Succeeded => Some((p, s))
      case _ => None
    })
    if (payments.isEmpty) {
      SwapPaymentStatus.NotSent
    } else if (succeeded.nonEmpty) {
      val (_, s) = succeeded.head
      SwapPaymentStatus.Succeeded(s.paymentPreimage, succeeded.map(_._1.amount).sum, succeeded.map(_._2.feesPaid).sum)
    } else if (payments.exists(_.status == OutgoingPaymentStatus.Pending)) {
      SwapPaymentStatus.Pending
    } else {
      val lastFailure = payments.map(_.status).collect { case f: OutgoingPaymentStatus.Failed => f.completedAt }.max
      if (failureFinal || lastFailure + RetryGracePeriod < now) SwapPaymentStatus.Failed else SwapPaymentStatus.Pending
    }
  }

}
