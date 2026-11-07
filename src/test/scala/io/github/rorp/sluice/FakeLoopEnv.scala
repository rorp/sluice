package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.bitcoin.scalacompat.{Block, BlockId, ByteVector32, Crypto, OutPoint, Satoshi, SatoshiLong, Script, Transaction, TxId, TxIn, TxOut}
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import fr.acinq.eclair.payment.Bolt11Invoice
import fr.acinq.eclair.payment.Bolt11Invoice.ExtraHop
import io.github.rorp.sluice.db.{L402Token, LoopDb, SwapRecord, SwapUpdate}
import io.github.rorp.sluice.swapserverrpc.PaymentFailureReason
import fr.acinq.eclair.transactions.Transactions
import fr.acinq.eclair.{BlockHeight, CltvExpiryDelta, MilliSatoshi, ShortChannelId, TimestampMilli, randomBytes32, randomKey}
import scodec.bits.ByteVector

import java.sql.SQLException
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/** A simple in-memory chain: a list of blocks and a mempool where conflicting transactions replace each other. */
class FakeLoopChain(initialHeight: BlockHeight) extends LoopChain {

  @volatile var height: BlockHeight = initialHeight
  @volatile var feerates: Int => FeeratePerKw = _ => FeeratePerKw(2500 sat)
  private var blocks = Map.empty[BlockHeight, Seq[Transaction]]
  /** Ids of the blocks that we mined: a block that replaces another one in a reorg gets a new id. */
  private var blockIds = Map.empty[BlockHeight, BlockId]
  private var mempool = Seq.empty[Transaction]
  /** Every transaction we were asked to publish. */
  @volatile var published = Seq.empty[Transaction]

  override def currentHeight(): BlockHeight = height

  private def blockId(h: BlockHeight): BlockId = synchronized(blockIds.getOrElse(h, BlockId(Crypto.sha256(ByteVector.fromLong(h.toLong)))))

  override def getBlock(h: BlockHeight): Future[ChainBlock] = synchronized {
    if (h > height) Future.failed(new IllegalArgumentException("Block height out of range"))
    else Future.successful(ChainBlock(blockId(h), blockId(h - 1), blocks.getOrElse(h, Nil)))
  }

  /** Transactions whose outputs our wallet spends to fund our htlcs: they confirmed long ago. */
  private var walletTxs = Set.empty[TxId]

  private def confirmedAt(txId: TxId): Option[BlockHeight] = synchronized {
    blocks.collectFirst { case (h, txs) if txs.exists(_.txid == txId) => h }.orElse(Option.when(walletTxs.contains(txId))(BlockHeight(0)))
  }

  private def confirmedTxs: Seq[Transaction] = synchronized(blocks.values.flatten.toSeq)

  override def getTxConfirmations(txId: TxId): Future[Option[Int]] = Future.successful {
    confirmedAt(txId).map(h => (height - h).toInt + 1).orElse(synchronized(mempool.find(_.txid == txId)).map(_ => 0))
  }

  override def isOutputSpendable(outPoint: OutPoint): Future[Boolean] = Future.successful {
    confirmedAt(outPoint.txid).nonEmpty && !confirmedTxs.exists(_.txIn.exists(_.outPoint == outPoint))
  }

  /** Our mempool rejects these transactions (their feerate is below its minimum for example). */
  @volatile var rejected: Transaction => Boolean = _ => false

  override def publish(tx: Transaction): Future[TxId] = synchronized {
    published = published :+ tx
    if (rejected(tx)) {
      Future.failed(new IllegalArgumentException("mempool min fee not met"))
    } else if (confirmedTxs.exists(_.txid == tx.txid)) {
      Future.successful(tx.txid)
    } else if (confirmedTxs.exists(_.txIn.exists(i => tx.txIn.exists(_.outPoint == i.outPoint)))) {
      Future.failed(new IllegalArgumentException("bad-txns-inputs-missingorspent"))
    } else {
      mempool = mempool.filterNot(_.txIn.exists(i => tx.txIn.exists(_.outPoint == i.outPoint))) :+ tx
      Future.successful(tx.txid)
    }
  }

  override def feerate(confTarget: Int): FeeratePerKw = feerates(confTarget)

  override def newSweepScript(): Future[ByteVector] = Future.successful(Script.write(Script.pay2wpkh(randomKey().publicKey)))

  /** Our wallet can't fund transactions when this is set. */
  @volatile var walletEmpty: Boolean = false
  @volatile var rolledBack = Seq.empty[Transaction]

  override def fundOutput(script: ByteVector, amount: Satoshi, feerate: FeeratePerKw, maxFee: Satoshi): Future[(Transaction, Satoshi)] = {
    val tx = FakeLoopChain.fundingTx(script, amount)
    synchronized { walletTxs = walletTxs ++ tx.txIn.map(_.outPoint.txid) }
    val fee = Transactions.weight2fee(feerate, 610)
    if (walletEmpty) Future.failed(new RuntimeException("insufficient funds"))
    else if (fee > maxFee) Future.failed(new RuntimeException(s"fee $fee exceeds budget $maxFee"))
    else Future.successful((tx, fee))
  }

  override def rollback(tx: Transaction): Future[Unit] = synchronized {
    rolledBack = rolledBack :+ tx
    Future.unit
  }

  def mempoolTxs: Seq[Transaction] = synchronized(mempool)

  def clearMempool(): Unit = synchronized {
    mempool = Nil
  }

  /** Mines a block with the mempool and the given transactions. */
  def mine(txs: Transaction*): BlockHeight = synchronized {
    height = height + 1
    blocks = blocks + (height -> (mempool ++ txs))
    blockIds = blockIds + (height -> BlockId(randomBytes32()))
    mempool = Nil
    height
  }

  def mineEmpty(n: Int): BlockHeight = synchronized {
    (1 to n).foreach(_ => {
      height = height + 1
      blocks = blocks + (height -> Nil)
      blockIds = blockIds + (height -> BlockId(randomBytes32()))
    })
    height
  }

  /** Removes the last block (its transactions are dropped). */
  def disconnectTip(): Unit = synchronized {
    blocks = blocks - height
    blockIds = blockIds - height
    height = height - 1
  }

}

object FakeLoopChain {
  /** A transaction paying to the given script (its input doesn't exist, which our fake chain doesn't check). */
  def fundingTx(script: ByteVector, amount: Satoshi): Transaction = Transaction(2, Seq(TxIn(OutPoint(TxId(randomBytes32()), 0), Nil, 0)), Seq(TxOut(10_000 sat, Script.write(Script.pay2wpkh(randomKey().publicKey))), TxOut(amount, script)), 0)
}

class FakeLoopPayments extends LoopPayments {

  private val nodeKey = randomKey()
  /** Our invoices and their status. */
  val invoices = TrieMap.empty[ByteVector32, (Bolt11Invoice, IncomingInvoiceStatus)]

  val statuses = TrieMap.empty[ByteVector32, SwapPaymentStatus]
  @volatile var paid = Seq.empty[(Bolt11Invoice, Satoshi)]
  /** The first hop of our payments, by payment hash. */
  val firstHops = TrieMap.empty[ByteVector32, Option[ShortChannelId]]
  /** The ids that eclair gave our payments, in order. */
  @volatile var paymentIds = Seq.empty[UUID]
  /** eclair fails our payments with this before sending them. */
  @volatile var payFailure_opt: Option[Throwable] = None

  override def pay(invoice: Bolt11Invoice, maxFee: Satoshi, firstHop_opt: Option[ShortChannelId]): Future[UUID] = synchronized {
    payFailure_opt.foreach(t => return Future.failed(t))
    paid = paid :+ (invoice, maxFee)
    firstHops.put(invoice.paymentHash, firstHop_opt)
    statuses.put(invoice.paymentHash, SwapPaymentStatus.Pending)
    val id = UUID.randomUUID()
    paymentIds = paymentIds :+ id
    Future.successful(id)
  }

  /** Payments whose parts all failed just now: eclair may still be retrying them, unless their failure is final. */
  @volatile var recentlyFailed = Set.empty[ByteVector32]

  override def status(paymentHash: ByteVector32, failureFinal: Boolean): SwapPaymentStatus =
    if (recentlyFailed.contains(paymentHash)) {
      if (failureFinal) SwapPaymentStatus.Failed else SwapPaymentStatus.Pending
    } else {
      statuses.getOrElse(paymentHash, SwapPaymentStatus.NotSent)
    }

  val failures = TrieMap.empty[ByteVector32, PaymentFailureInfo]

  override def failureInfo(paymentHash: ByteVector32): PaymentFailureInfo =
    failures.getOrElse(paymentHash, PaymentFailureInfo(PaymentFailureReason.LND_FAILURE_REASON_ERROR, Nil))

  @volatile var canSendNow = true

  override def canSend(): Future[Boolean] = Future.successful(canSendNow)

  /** Our channels by the id that eclair's router uses, with how much they can send now (0 msat while they're offline). */
  @volatile var channelBalances = Map.empty[ShortChannelId, MilliSatoshi]
  /** Other ids of our channels (e.g. the local alias of an announced channel), with the id that eclair's router uses. */
  @volatile var otherChannelIds = Map.empty[ShortChannelId, ShortChannelId]

  override def firstHopChannel(scid: ShortChannelId): Future[Option[FirstHopChannel]] = Future.successful {
    val routerScid = otherChannelIds.getOrElse(scid, scid)
    channelBalances.get(routerScid).map(FirstHopChannel(routerScid, _))
  }

  /** Payments that eclair forgot (it stopped before committing their htlcs). */
  @volatile var orphaned = Set.empty[ByteVector32]
  @volatile var orphanChecks = Seq.empty[Set[ByteVector32]]

  override def failOrphanedPayments(paymentHashes: Set[ByteVector32]): Seq[ByteVector32] = synchronized {
    orphanChecks = orphanChecks :+ paymentHashes
    val found = (paymentHashes intersect orphaned).toSeq
    found.foreach(statuses.remove)
    orphaned = orphaned -- found
    found
  }

  /** Routing hints for our private channels, by peer. */
  @volatile var privateChannelHints = Map.empty[PublicKey, ExtraHop]

  override def routeHints(lastHop_opt: Option[PublicKey]): Future[List[List[ExtraHop]]] =
    Future.successful(privateChannelHints.collect { case (peer, hop) if lastHop_opt.forall(_ == peer) => hop :: Nil }.toList)

  override def createInvoice(preimage: ByteVector32, amount: MilliSatoshi, description: String, expiry: FiniteDuration, routeHints: List[List[ExtraHop]]): Future[Bolt11Invoice] = {
    val invoice = unregisteredInvoice(Crypto.sha256(preimage), amount, description, routeHints)
    invoices.put(invoice.paymentHash, (invoice, IncomingInvoiceStatus.Pending))
    Future.successful(invoice)
  }

  /** We throw this when creating invoices, when set. */
  @volatile var invoiceFailure_opt: Option[Throwable] = None

  override def unregisteredInvoice(paymentHash: ByteVector32, amount: MilliSatoshi, description: String, routeHints: List[List[ExtraHop]]): Bolt11Invoice = {
    invoiceFailure_opt.foreach(t => throw t)
    Bolt11Invoice(Block.RegtestGenesisBlock.hash, Some(amount), paymentHash, nodeKey, Left(description), CltvExpiryDelta(18), extraHops = routeHints)
  }

  /** The next calls to incomingStatus throw (eclair's payments db is unavailable). */
  @volatile var incomingStatusFailures = 0

  override def incomingStatus(paymentHash: ByteVector32): IncomingInvoiceStatus = {
    synchronized {
      if (incomingStatusFailures > 0) {
        incomingStatusFailures -= 1
        throw new SQLException("payments db is unavailable")
      }
    }
    invoices.get(paymentHash).map(_._2).getOrElse(IncomingInvoiceStatus.Unknown)
  }

  override def removeInvoice(paymentHash: ByteVector32): Try[Unit] = synchronized {
    invoices.get(paymentHash) match {
      case Some((_, _: IncomingInvoiceStatus.Received)) => Failure(new IllegalArgumentException("Cannot remove a received incoming payment"))
      case _ =>
        invoices.remove(paymentHash)
        Success(())
    }
  }

  /** Someone pays our invoice: fails if it doesn't exist anymore. */
  def receive(paymentHash: ByteVector32): Boolean = synchronized {
    invoices.get(paymentHash) match {
      case Some((invoice, IncomingInvoiceStatus.Pending)) =>
        invoices.put(paymentHash, (invoice, IncomingInvoiceStatus.Received(invoice.amount_opt.get)))
        true
      case _ => false
    }
  }

}

/** Our db, whose operations fail when we ask them to (the db is unavailable). */
class FlakyLoopDb(db: LoopDb) extends LoopDb {

  private val failures = TrieMap.empty[String, Int]

  /** The next n calls to this operation fail. */
  def failNext(operation: String, n: Int = 1): Unit = failures.put(operation, n)

  private def check(operation: String): Unit = synchronized {
    failures.get(operation).filter(_ > 0).foreach { n =>
      failures.put(operation, n - 1)
      throw new SQLException(s"db is unavailable ($operation)")
    }
  }

  override def addSwap(swap: SwapRecord): Unit = { check("addSwap"); db.addSwap(swap) }

  override def updateSwap(swapHash: ByteVector32, state: String, contract: ByteVector, updatedAt: TimestampMilli): Unit = { check("updateSwap"); db.updateSwap(swapHash, state, contract, updatedAt) }

  override def getSwap(swapHash: ByteVector32): Option[SwapRecord] = { check("getSwap"); db.getSwap(swapHash) }

  override def getSwapByIdempotencyKey(idempotencyKey: String): Option[SwapRecord] = { check("getSwapByIdempotencyKey"); db.getSwapByIdempotencyKey(idempotencyKey) }

  override def listSwaps(): Seq[SwapRecord] = { check("listSwaps"); db.listSwaps() }

  override def listSwapUpdates(swapHash: ByteVector32): Seq[SwapUpdate] = db.listSwapUpdates(swapHash)

  override def nextKeyIndex(): Long = { check("nextKeyIndex"); db.nextKeyIndex() }

  override def storeL402Token(token: L402Token): Unit = db.storeL402Token(token)

  override def currentL402Token(): Option[L402Token] = { check("currentL402Token"); db.currentL402Token() }

  override def listL402Tokens(): Seq[L402Token] = db.listL402Tokens()

  override def removePendingL402Token(paymentHash: ByteVector32): Unit = db.removePendingL402Token(paymentHash)

  override def close(): Unit = db.close()

}
