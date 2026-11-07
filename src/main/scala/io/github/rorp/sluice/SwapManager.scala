package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.bitcoin.scalacompat.{BlockHash, ByteVector32, Crypto, Satoshi, SatoshiLong, Script, TxId, addressFromPublicKeyScript, addressToPublicKeyScript}
import fr.acinq.eclair.payment.Bolt11Invoice
import fr.acinq.eclair.payment.Bolt11Invoice.ExtraHop
import io.github.rorp.sluice.SwapServerClient.{LoopInTerms, LoopOutTerms}
import io.github.rorp.sluice.db.{LoopDb, SwapRecord, SwapRequestId, SwapType}
import fr.acinq.eclair.transactions.Transactions
import fr.acinq.eclair.{Alias, BlockHeight, MilliSatoshi, ShortChannelId, TimestampMilli, TimestampSecond, ToMilliSatoshiConversion, randomBytes32}
import scodec.bits.ByteVector
import grizzled.slf4j.Logging
import io.grpc.{Status, StatusRuntimeException}

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object SwapManager {

  case class LoopOutQuote(amount: Satoshi,
                          swapFee: Satoshi,
                          prepayAmount: Satoshi,
                          swapPaymentDest: PublicKey,
                          confTarget: Int,
                          cltvExpiry: BlockHeight,
                          swapPublicationDeadline: TimestampSecond)

  /** @param htlcPublishFee estimated fee of our htlc transaction (it depends on the inputs eclair's wallet will use). */
  case class LoopInQuote(amount: Satoshi, swapFee: Satoshi, cltvDelta: Int, htlcConfTarget: Int, htlcPublishFee: Satoshi)

  /** What we expose about L402 tokens (the macaroon and preimage are credentials and stay private). */
  case class L402TokenInfo(paymentHash: ByteVector32, pending: Boolean, amountPaid: MilliSatoshi, routingFeePaid: MilliSatoshi, createdAt: TimestampMilli)

  // Defaults from loopd and the loop cli.
  val MinConfTarget = 2
  /** bitcoind's longest fee estimate (we couldn't persist a loop in's conf target above 65535 anyway). */
  val MaxConfTarget = 1008
  /** If the swap isn't "fast", the server may wait this long before publishing the htlc to batch it with other swaps. */
  val DefaultSwapWaitTime = 30.minutes
  /** Our loop in invoice must not expire before the server pays it (loopd uses one year). */
  val LoopInInvoiceExpiry = 365.days

  /** loopd's getExpiry (client.go): conf targets below the server's minimum delta use the minimum delta. */
  /** eclair's block height may lag behind the server's: the server would see a cltv delta below its minimum. */
  val LoopOutExpiryMargin = 3

  def loopOutExpiry(currentHeight: BlockHeight, terms: LoopOutTerms, confTarget: Int): Either[String, BlockHeight] = {
    if (confTarget > terms.maxCltvDelta) Left(s"confirmation target $confTarget exceeds maximum server cltv delta of ${terms.maxCltvDelta}")
    else Right(currentHeight + (confTarget.max(terms.minCltvDelta) + LoopOutExpiryMargin).min(terms.maxCltvDelta))
  }

  /**
   * Parameters of a new loop out, the limits default to what the loop cli uses (the server fees to its quote, if within
   * our configured limits, see [[checkLoopOutQuote]]).
   *
   * @param address_opt where to send the swap amount (a new address of eclair's wallet by default).
   * @param htlcConfirmations_opt confirmations of the server's htlc before we reveal the preimage (loop.loop-out-htlc-confirmations
   *                              by default): more confirmations protect large swaps against reorgs of the htlc.
   * @param firstHop_opt          pay the swap and prepay invoices through this channel of ours only (its local alias or
   *                              real scid), like loopd's outgoing channel set (but only one).
   * @param idempotencyKey_opt    a retry of this request with the same key returns its swap instead of creating another one
   *                              (see SwapManager.createOnce).
   */
  case class LoopOutRequest(amount: Satoshi,
                            confTarget_opt: Option[Int] = None,
                            fast: Boolean = false,
                            address_opt: Option[String] = None,
                            maxSwapFee_opt: Option[Satoshi] = None,
                            maxPrepayAmount_opt: Option[Satoshi] = None,
                            maxMinerFee_opt: Option[Satoshi] = None,
                            maxSwapRoutingFee_opt: Option[Satoshi] = None,
                            maxPrepayRoutingFee_opt: Option[Satoshi] = None,
                            htlcConfirmations_opt: Option[Int] = None,
                            firstHop_opt: Option[ShortChannelId] = None,
                            idempotencyKey_opt: Option[String] = None)

  case class LoopOutDetails(htlcAddress: String,
                            sweepAddress: String,
                            sweepConfTarget: Int,
                            htlcConfirmations: Int,
                            firstHop: Option[ShortChannelId],
                            swapFee: Satoshi,
                            prepayAmount: Satoshi,
                            limits: LoopOutLimits,
                            publicationDeadline: TimestampSecond,
                            htlcOutPoint: Option[String],
                            htlcAmount: Option[Satoshi],
                            sweepTxIds: Seq[TxId],
                            minerFee: Option[Satoshi],
                            swapRoutingFee: Option[MilliSatoshi],
                            prepayRoutingFee: Option[MilliSatoshi])

  /**
   * Parameters of a new loop in, the limits default to what the loop cli uses (the server fee to its quote, if within our
   * configured limits, see [[checkQuotedSwapFee]]).
   *
   * @param address_opt where to send our funds back if the swap fails (a new address of eclair's wallet by default).
   * @param routeHints  give the server routing hints for our private channels (only with lastHop if set), in its probe
   *                    and quote requests and in our invoices, so that it can pay us through them (required if we
   *                    don't have public channels), like loopd's private flag.
   * @param idempotencyKey_opt a retry of this request with the same key returns its swap instead of creating another one
   *                           (see SwapManager.createOnce).
   */
  case class LoopInRequest(amount: Satoshi,
                           htlcConfTarget_opt: Option[Int] = None,
                           lastHop_opt: Option[PublicKey] = None,
                           address_opt: Option[String] = None,
                           maxSwapFee_opt: Option[Satoshi] = None,
                           maxMinerFee_opt: Option[Satoshi] = None,
                           routeHints: Boolean = false,
                           idempotencyKey_opt: Option[String] = None)

  case class LoopInDetails(htlcAddress: String,
                           refundAddress: String,
                           htlcConfTarget: Int,
                           lastHop: Option[PublicKey],
                           swapFee: Satoshi,
                           limits: LoopInLimits,
                           htlcOutPoint: Option[String],
                           htlcTxFee: Option[Satoshi],
                           refundTxIds: Seq[TxId],
                           refundFee: Option[Satoshi],
                           amountReceived: Option[MilliSatoshi])

  case class SwapInfo(swapHash: ByteVector32,
                      swapType: String,
                      state: String,
                      amount: Satoshi,
                      cltvExpiry: BlockHeight,
                      createdAt: TimestampMilli,
                      updatedAt: TimestampMilli,
                      idempotencyKey: Option[String],
                      loopOut: Option[LoopOutDetails],
                      loopIn: Option[LoopInDetails])

  // Defaults of the loop cli (cmd/loop/main.go).
  val MaxRoutingFeeBase: Satoshi = 10.sat
  val MaxRoutingFeeRatePpm = 20_000
  /** Applied to the estimated miner fee of our sweep, so that the swap doesn't fail if fees increase. */
  val MinerFeeMultiplier = 250
  /** Applied to the estimated miner fee of our loop in htlc. */
  val LoopInMinerFeeMultiplier = 3

  def maxRoutingFee(amount: Satoshi): Satoshi = MaxRoutingFeeBase + amount * MaxRoutingFeeRatePpm / 1_000_000

  /**
   * Limits that the user didn't set default to the server's quote, which we only accept within our configured limits:
   * the server could otherwise charge any fee (we don't ask the user to confirm the quote like the loop cli does).
   */
  def checkQuotedSwapFee(amount: Satoshi, swapFee: Satoshi, maxSwapFee_opt: Option[Satoshi], config: SluiceConfig): Either[String, Unit] = maxSwapFee_opt match {
    case Some(maxSwapFee) => Either.cond(swapFee <= maxSwapFee, (), s"swap fee $swapFee exceeds maximum $maxSwapFee")
    case None => Either.cond(swapFee <= config.maxSwapFee(amount), (), s"swap fee $swapFee exceeds ${config.maxSwapFee(amount)} (loop.max-swap-fee-ppm=${config.maxSwapFeePpm}): set maxSwapFeeSat to accept it")
  }

  /** Like [[checkQuotedSwapFee]], for the prepay too: the server can take it without doing the swap. */
  def checkLoopOutQuote(request: LoopOutRequest, quote: LoopOutQuote, config: SluiceConfig): Either[String, Unit] = for {
    _ <- checkQuotedSwapFee(request.amount, quote.swapFee, request.maxSwapFee_opt, config)
    _ <- request.maxPrepayAmount_opt match {
      case Some(maxPrepay) => Either.cond(quote.prepayAmount <= maxPrepay, (), s"prepay amount ${quote.prepayAmount} exceeds maximum $maxPrepay")
      case None => Either.cond(quote.prepayAmount <= config.maxPrepay, (), s"prepay amount ${quote.prepayAmount} exceeds ${config.maxPrepay} (loop.max-prepay-sat): set maxPrepayAmountSat to accept it")
    }
  } yield ()

  /** loopd's validateLoopOutContract, plus sanity checks on the invoices. */
  def validateLoopOut(swapHash: ByteVector32, amount: Satoshi, limits: LoopOutLimits, swapInvoice: Bolt11Invoice, prepayInvoice: Bolt11Invoice, chainHash: BlockHash): Either[String, Unit] = {
    val swapAmount = swapInvoice.amount_opt.map(_.truncateToSatoshi)
    val prepayAmount = prepayInvoice.amount_opt.map(_.truncateToSatoshi)
    for {
      _ <- Either.cond(swapInvoice.paymentHash == swapHash, (), s"swap invoice hash ${swapInvoice.paymentHash} doesn't match swap hash")
      _ <- Either.cond(Seq(swapInvoice, prepayInvoice).forall(i => Bolt11Invoice.prefixes.get(chainHash).contains(i.prefix)), (), "invoices are for another chain")
      _ <- Either.cond(swapAmount.nonEmpty && prepayAmount.nonEmpty, (), "invoices without amount")
      swapFee = swapAmount.get + prepayAmount.get - amount
      _ <- Either.cond(swapFee <= limits.maxSwapFee, (), s"swap fee $swapFee exceeds maximum ${limits.maxSwapFee}")
      _ <- Either.cond(prepayAmount.get <= limits.maxPrepayAmount, (), s"prepay amount ${prepayAmount.get} exceeds maximum ${limits.maxPrepayAmount}")
      _ <- Either.cond(!swapInvoice.isExpired() && !prepayInvoice.isExpired(), (), "invoices are expired")
    } yield ()
  }

  /** loopd's ValidateLoopInContract, plus sanity checks on the server's keys. */
  def validateLoopIn(currentHeight: BlockHeight, expiry: BlockHeight): Either[String, Unit] = {
    if (expiry - currentHeight > LoopIn.MaxLoopInAcceptDelta) Left(s"htlc expiry $expiry is too far in the future (current height $currentHeight)")
    else if (expiry < currentHeight + LoopIn.MinLoopInExpiryDelta) Left(s"htlc expiry $expiry is too soon (current height $currentHeight)")
    else Right(())
  }

  /** The hash of the probe invoice we give the server, as loopd does: it must never be paid. */
  def probeHash(swapHash: ByteVector32): ByteVector32 = {
    val h = Crypto.sha256(swapHash)
    ByteVector32(h.bytes.update(0, (h.bytes(0) ^ 1).toByte))
  }

  /**
   * A short channel id as eclair displays it: a real scid as coordinates (800000x1x0) or an alias in hex (0x1b865d25d5a34f4).
   * eclair's firstHop parameter only accepts coordinates, which aren't shown for the local alias of our channels.
   */
  def parseShortChannelId(s: String): Try[ShortChannelId] =
    if (s.startsWith("0x")) Try(Alias(java.lang.Long.parseUnsignedLong(s.drop(2), 16))) else ShortChannelId.fromCoordinates(s)

  /** Like eclair's payment external ids. */
  val MaxIdempotencyKeyLength = 64

  def checkIdempotencyKey(key: String): Either[String, Unit] = {
    if (key.isEmpty) Left("idempotency key must not be empty")
    else if (key.length > MaxIdempotencyKeyLength) Left(s"idempotency key must be at most $MaxIdempotencyKeyLength characters")
    else if (key.exists(_.isControl)) Left("idempotency key must not contain control characters")
    else Right(())
  }

  /**
   * Hash of the parameters of a request, as the caller gave them (before we apply defaults), except its idempotency key.
   * Parameters that weren't given aren't hashed: adding new optional parameters doesn't change the hash of requests that
   * don't use them.
   */
  private def requestHash(swapType: SwapType, parameters: Seq[(String, Option[Any])]): ByteVector32 = {
    // Values are prefixed with their length, so that two different requests can't have the same encoding.
    val encoded = (("swapType" -> Some(swapType.name)) +: parameters)
      .collect { case (name, Some(value)) => name -> value.toString }
      .sortBy(_._1)
      .map { case (name, value) => s"$name=${value.length}:$value" }
      .mkString(";")
    Crypto.sha256(ByteVector.view(encoded.getBytes(StandardCharsets.UTF_8)))
  }

  def requestHash(request: LoopOutRequest): ByteVector32 = requestHash(SwapType.LoopOut, Seq(
    "amount" -> Some(request.amount.toLong),
    "confTarget" -> request.confTarget_opt,
    "fast" -> Option.when(request.fast)(true),
    "address" -> request.address_opt,
    "maxSwapFee" -> request.maxSwapFee_opt.map(_.toLong),
    "maxPrepayAmount" -> request.maxPrepayAmount_opt.map(_.toLong),
    "maxMinerFee" -> request.maxMinerFee_opt.map(_.toLong),
    "maxSwapRoutingFee" -> request.maxSwapRoutingFee_opt.map(_.toLong),
    "maxPrepayRoutingFee" -> request.maxPrepayRoutingFee_opt.map(_.toLong),
    "htlcConfirmations" -> request.htlcConfirmations_opt,
    "firstHop" -> request.firstHop_opt.map(_.toLong),
  ))

  def requestHash(request: LoopInRequest): ByteVector32 = requestHash(SwapType.LoopIn, Seq(
    "amount" -> Some(request.amount.toLong),
    "htlcConfTarget" -> request.htlcConfTarget_opt,
    "lastHop" -> request.lastHop_opt.map(_.toHex),
    "address" -> request.address_opt,
    "maxSwapFee" -> request.maxSwapFee_opt.map(_.toLong),
    "maxMinerFee" -> request.maxMinerFee_opt.map(_.toLong),
    "routeHints" -> Option.when(request.routeHints)(true),
  ))

  def checkAmount(amount: Satoshi, min: Satoshi, max: Satoshi): Either[String, Unit] = {
    if (amount < min) Left(s"swap amount too low (min=$min)")
    else if (amount > max) Left(s"swap amount too high (max=$max)")
    else Right(())
  }

}

/**
 * Entry point for the plugin's API: creates swaps, resumes them after a restart and lists them.
 *
 * @param spawn          starts the state machine of a swap.
 * @param abandonRunning asks the state machine of a running swap to abandon it: replies with the reason why it can't.
 */
class SwapManager(server: SwapServerClient,
                  db: LoopDb,
                  config: SluiceConfig,
                  nodeId: PublicKey,
                  chain: LoopChain,
                  payments: LoopPayments,
                  keys: LoopKeys,
                  chainHash: BlockHash,
                  spawn: SwapLike => Unit,
                  abandonRunning: ByteVector32 => Future[Either[String, SwapRecord]] = _ => Future.successful(Left("swap isn't running")))(implicit ec: ExecutionContext) extends Logging {

  import SwapManager._

  private def fail[T](msg: String): Future[T] = Future.failed(new IllegalArgumentException(msg))

  private def currentBlockHeight(): BlockHeight = chain.currentHeight()

  /** Key indexes of swaps being created (not yet in the db). */
  private var reservedKeyIndexes = Set.empty[Long]

  private def reserveKeyIndex(): Long = synchronized {
    val index = (reservedKeyIndexes.maxOption.map(_ + 1).toSeq :+ db.nextKeyIndex()).max
    reservedKeyIndexes += index
    index
  }

  private def releaseKeyIndex(index: Long): Unit = synchronized {
    reservedKeyIndexes -= index
  }

  /**
   * Creates a swap with a key index that no other swap uses: we reserve it until the swap is in the db, or failed. The
   * creation logs its failures with the swap hash, except those it throws instead of returning a failed future (and our
   * failures to reserve a key index): we log those with the given context.
   */
  private def withKeyIndex(context: String)(create: Long => Future[SwapInfo]): Future[SwapInfo] = {
    val created = for {
      keyIndex <- Try(reserveKeyIndex())
      f <- Try(create(keyIndex)).recoverWith { case t =>
        releaseKeyIndex(keyIndex)
        Failure(t)
      }
    } yield f.andThen { case _ => releaseKeyIndex(keyIndex) }
    created match {
      case Success(f) => f
      case Failure(t) =>
        logCreationFailure(context, t)
        Future.failed(t)
    }
  }

  /** Creations in progress of requests that have an idempotency key, by key. */
  private var creating = Map.empty[String, (SwapRequestId, Future[SwapInfo])]

  /**
   * Creates the swap of a request at most once, when it has an idempotency key: API calls time out while we're still
   * creating their swap, and the swap may be created anyway. A retry of the same request (same key and parameters)
   * returns the swap it created, whatever its state, or waits for its creation if it is still in progress, before or
   * after a restart. A request that failed before its swap was persisted can be retried with the same key. A key can't
   * be used for another request (other parameters, or another swap type), even after its swap failed.
   */
  private def createOnce(context: String, requestId_opt: Option[SwapRequestId])(create: => Future[SwapInfo]): Future[SwapInfo] = requestId_opt match {
    case None => create
    case Some(requestId) =>
      val key = requestId.idempotencyKey
      val promise = Promise[SwapInfo]()
      // A retry either finds our creation in progress, or our swap in the db (we persist it before our creation ends).
      val previous = Try(synchronized {
        creating.get(key) match {
          case Some((requestId1, inProgress)) => Some(Left((requestId1, inProgress)))
          case None => db.getSwapByIdempotencyKey(key) match {
            case Some(record) => Some(Right(record))
            case None =>
              creating += key -> (requestId, promise.future)
              None
          }
        }
      })
      val reused = s"idempotency key $key was already used for another request"
      previous match {
        case Failure(t) =>
          logCreationFailure(context, t)
          Future.failed(t)
        case Success(Some(Left((requestId1, inProgress)))) if requestId1 == requestId =>
          logger.info(s"idempotency key $key: the request was retried, waiting for the creation of its swap")
          inProgress
        case Success(Some(Right(record))) if record.requestId_opt.contains(requestId) =>
          logger.info(s"idempotency key $key: the request was retried, it created swap ${record.swapHash} (state=${record.state})")
          Future.fromTry(Try(swapInfo(record)))
        case Success(Some(_)) =>
          logger.warn(s"$context: $reused")
          fail(reused)
        case Success(None) =>
          promise.completeWith(Future.unit.flatMap(_ => create))
          promise.future.onComplete(_ => synchronized(creating -= key))
          promise.future
      }
  }

  /** Validates the request's idempotency key, then creates its swap at most once (see [[createOnce]]). */
  private def withIdempotencyKey(context: String, idempotencyKey_opt: Option[String], requestHash: => ByteVector32)(create: Option[SwapRequestId] => Future[SwapInfo]): Future[SwapInfo] = {
    idempotencyKey_opt.map(checkIdempotencyKey).getOrElse(Right(())) match {
      case Left(error) =>
        logger.warn(s"$context: $error")
        fail(error)
      case Right(()) =>
        val requestId_opt = idempotencyKey_opt.map(SwapRequestId(_, requestHash))
        createOnce(context, requestId_opt)(create(requestId_opt))
    }
  }

  /** Restarts the state machines of pending swaps. */
  def resumeSwaps(): Unit = {
    val records = db.listSwaps()
    // Payments interrupted by a restart before all their htlcs were committed must be sent again: right away if none of
    // their htlcs was committed, otherwise once the server's node has failed those that were.
    val loopOuts = records.collect { case r if r.swapType == SwapType.LoopOut && !LoopOut.isFinal(r.state) => LoopOut.Swap(r) }
    val paymentHashes = loopOuts.flatMap(s => Seq(s.swapHash, Bolt11Invoice.fromString(s.contract.prepayInvoice).get.paymentHash)) ++ db.currentL402Token().filter(_.isPending).map(_.paymentHash)
    payments.failOrphanedPayments(paymentHashes.toSet).foreach(h => logger.warn(s"payment $h was interrupted before its htlcs were committed, it will be sent again (once its committed htlcs, if any, have failed)"))
    records.foreach { record =>
      record.swapType match {
        case SwapType.LoopOut if !LoopOut.isFinal(record.state) =>
          logger.info(s"resuming loop out ${record.swapHash} (state=${record.state})")
          spawn(LoopOut.Swap(record))
        case SwapType.LoopIn if !LoopIn.isFinal(record.state) =>
          logger.info(s"resuming loop in ${record.swapHash} (state=${record.state})")
          spawn(LoopIn.Swap(record))
        case _ => ()
      }
    }
  }

  def loopOutTerms(): Future[LoopOutTerms] = server.loopOutTerms()

  def loopOutQuote(amount: Satoshi, confTarget_opt: Option[Int], fast: Boolean): Future[LoopOutQuote] = {
    val confTarget = confTarget_opt.getOrElse(config.defaultSweepConfTarget)
    if (confTarget < MinConfTarget) return fail(s"confirmation target must be at least $MinConfTarget")
    val deadline = if (fast) TimestampSecond.now() else TimestampSecond.now() + DefaultSwapWaitTime
    for {
      terms <- server.loopOutTerms()
      _ <- checkAmount(amount, terms.minSwapAmount, terms.maxSwapAmount).fold(fail, Future.successful)
      expiry <- loopOutExpiry(currentBlockHeight(), terms, confTarget).fold(fail, Future.successful)
      quote <- server.loopOutQuote(amount, expiry, deadline.toLong)
    } yield LoopOutQuote(amount, quote.swapFee, quote.prepayAmount, quote.swapPaymentDest, confTarget, expiry, deadline)
  }

  /** Creates a loop out swap with the server and starts it: we pay the server right away. */
  def loopOut(request: LoopOutRequest): Future[SwapInfo] = {
    val context = s"cannot create loop out amount=${request.amount}"
    withIdempotencyKey(context, request.idempotencyKey_opt, requestHash(request))(createLoopOut(context, request, _))
  }

  private def createLoopOut(context: String, request: LoopOutRequest, requestId_opt: Option[SwapRequestId]): Future[SwapInfo] = {
    val prepared = for {
      quote <- loopOutQuote(request.amount, request.confTarget_opt, request.fast)
      _ <- checkLoopOutQuote(request, quote, config).fold(fail, Future.successful)
      firstHop_opt <- request.firstHop_opt.map(checkFirstHop(_, quote).map(Some(_))).getOrElse(Future.successful(None))
      sweepScript <- request.address_opt match {
        case Some(address) => addressToPublicKeyScript(chainHash, address).map(Script.write).fold(e => fail(s"invalid address: $e"), Future.successful)
        case None => chain.newSweepScript()
      }
    } yield (quote, firstHop_opt, sweepScript)
    // Failures past this point have a swap hash: createLoopOut logs them.
    prepared.transformWith {
      case Failure(t) =>
        logCreationFailure(context, t)
        Future.failed(t)
      case Success((quote, firstHop_opt, sweepScript)) =>
        withKeyIndex(context)(createLoopOut(request.copy(firstHop_opt = firstHop_opt), quote, sweepScript, requestId_opt, _))
    }
  }

  /**
   * Our payments would fail if they can't go through the first hop: we check it before creating the swap. Its balance
   * must cover both invoices, whose total is the swap amount plus the swap fee (routing fees come on top of that).
   *
   * @return the id that eclair's router uses for this channel.
   */
  private def checkFirstHop(firstHop: ShortChannelId, quote: LoopOutQuote): Future[ShortChannelId] = {
    val needed = (quote.amount + quote.swapFee).toMilliSatoshi
    payments.firstHopChannel(firstHop).flatMap {
      case None => fail(s"first hop $firstHop isn't one of our open channels")
      case Some(channel) if channel.canSend < needed => fail(s"our channel $firstHop can only send ${channel.canSend} now (its balance, within the value of htlcs in flight that our peer accepts), the swap needs $needed (swap amount + fee)")
      case Some(channel) => Future.successful(channel.routerScid)
    }
  }

  private def createLoopOut(request: LoopOutRequest, quote: LoopOutQuote, sweepScript: ByteVector, requestId_opt: Option[SwapRequestId], keyIndex: Long): Future[SwapInfo] = {
    val preimage = randomBytes32()
    val swapHash = Crypto.sha256(preimage)
    val receiverKey = keys.swapKey(keyIndex).publicKey
    // The fee of a script path sweep (heavier than a cooperative sweep) at our confirmation target.
    val dummyHtlc = Htlc(swapHash, quote.cltvExpiry, receiverKey, receiverKey, receiverKey, receiverKey)
    val sweepFeeEstimate = Transactions.weight2fee(chain.feerate(quote.confTarget), dummyHtlc.sweepWeight(sweepScript, keyPath = false))
    val limits = LoopOutLimits(
      maxSwapFee = request.maxSwapFee_opt.getOrElse(quote.swapFee),
      maxPrepayAmount = request.maxPrepayAmount_opt.getOrElse(quote.prepayAmount),
      maxMinerFee = request.maxMinerFee_opt.getOrElse((sweepFeeEstimate * MinerFeeMultiplier).min(LoopOut.maxFeeToAmount(request.amount))),
      maxSwapRoutingFee = request.maxSwapRoutingFee_opt.getOrElse(maxRoutingFee(request.amount)),
      maxPrepayRoutingFee = request.maxPrepayRoutingFee_opt.getOrElse(maxRoutingFee(quote.prepayAmount)),
    )
    val initiationHeight = currentBlockHeight()
    val htlcConfirmations = request.htlcConfirmations_opt.getOrElse(config.loopOutHtlcConfirmations)
    val revealDeadline = quote.cltvExpiry - LoopOut.MinPreimageRevealDelta
    logger.info(s"creating loop out $swapHash amount=${request.amount} expiry=${quote.cltvExpiry} htlc_confirmations=$htlcConfirmations limits=$limits${request.firstHop_opt.map(c => s" first_hop=$c").getOrElse("")}")
    val checks: Either[String, Unit] =
      // We don't reveal the preimage if we can't pay for our sweep: such a swap would fail and lose the prepay.
      if (limits.maxMinerFee < sweepFeeEstimate) Left(s"max miner fee ${limits.maxMinerFee} is below the estimated fee of our sweep ($sweepFeeEstimate)")
      // The htlc can't confirm before the next block, and we must reveal the preimage before the reveal deadline.
      else if (htlcConfirmations < 1) Left("htlc confirmations must be at least 1")
      else if (initiationHeight + htlcConfirmations >= revealDeadline) Left(s"htlc confirmations $htlcConfirmations leave no time to reveal the preimage before height $revealDeadline (htlc expiry=${quote.cltvExpiry}): use a higher confirmation target")
      else Right(())
    val response_f = checks.fold(fail, Future.successful).flatMap(_ => server.newLoopOutSwap(swapHash, request.amount, receiverKey, quote.cltvExpiry, quote.swapPublicationDeadline.toLong))
    response_f.flatMap { response =>
      val result = for {
        swapInvoice <- Bolt11Invoice.fromString(response.swapInvoice).toEither.left.map(e => s"invalid swap invoice: ${e.getMessage}")
        prepayInvoice <- Bolt11Invoice.fromString(response.prepayInvoice).toEither.left.map(e => s"invalid prepay invoice: ${e.getMessage}")
        _ <- validateLoopOut(swapHash, request.amount, limits, swapInvoice, prepayInvoice, chainHash)
      } yield {
        val contract = LoopOutContract(response.swapInvoice, response.prepayInvoice, response.serverKey, sweepScript, quote.confTarget, htlcConfirmations, limits, initiationHeight, quote.swapPublicationDeadline, request.firstHop_opt)
        val progress = LoopOutProgress(scannedHeight = initiationHeight - 1, scannedBlockId_opt = None, htlc_opt = None, preimagePushed = false, sweepTxs = Nil, musig2Failures = 0)
        val data = LoopOutData(contract, progress)
        val now = TimestampMilli.now()
        val record = SwapRecord(swapHash, SwapType.LoopOut, LoopOut.States.Initiated, request.amount, preimage, keyIndex, quote.cltvExpiry, LoopOutData.encode(data), now, now, requestId_opt)
        db.addSwap(record)
        if (response.serverMessage.nonEmpty) logger.info(s"loop out $swapHash: server message: ${response.serverMessage}")
        spawn(LoopOut.Swap(record, data))
        swapInfo(record)
      }
      result.fold(fail, Future.successful)
    }.andThen { case Failure(t) => logCreationFailure(s"cannot create loop out $swapHash", t) }
  }

  /** Stops a pending swap, when that is safe (the swap's state machine decides). */
  def abandonSwap(swapHash: ByteVector32): Future[SwapInfo] = db.getSwap(swapHash) match {
    case None => fail(s"unknown swap $swapHash")
    case Some(record) if isFinal(record) => fail(s"swap is already final (${record.state})")
    case Some(_) => abandonRunning(swapHash).flatMap {
      case Left(reason) => fail(s"cannot abandon swap: $reason")
      case Right(record) => Future.successful(swapInfo(record))
    }
  }

  private def isFinal(record: SwapRecord): Boolean = record.swapType match {
    case SwapType.LoopOut => LoopOut.isFinal(record.state)
    case SwapType.LoopIn => LoopIn.isFinal(record.state)
  }

  def listSwaps(): Seq[SwapInfo] = db.listSwaps().map(swapInfo)

  def getSwap(swapHash: ByteVector32): Option[SwapInfo] = db.getSwap(swapHash).map(swapInfo)

  private def swapInfo(record: SwapRecord): SwapInfo = {
    val loopOut_opt = record.swapType match {
      case SwapType.LoopOut =>
        val swap = LoopOut.Swap(record)
        val contract = swap.contract
        val receiverKey = keys.swapKey(record.keyIndex).publicKey
        val htlc = Htlc(record.swapHash, record.cltvExpiry, contract.serverKey, receiverKey, contract.serverKey, receiverKey)
        val swapInvoice = Bolt11Invoice.fromString(contract.swapInvoice).get
        val prepayInvoice = Bolt11Invoice.fromString(contract.prepayInvoice).get
        val prepayAmount = prepayInvoice.amount_opt.get.truncateToSatoshi
        val htlcAmount_opt = swap.progress.htlc_opt.map(_.txOut.amount)
        val routingFee = (paymentHash: ByteVector32) => payments.status(paymentHash, failureFinal = true) match {
          case s: SwapPaymentStatus.Succeeded => Some(s.fees)
          case _ => None
        }
        Some(LoopOutDetails(
          htlcAddress = htlc.address(chainHash),
          sweepAddress = addressFromPublicKeyScript(chainHash, Script.parse(contract.sweepScript)).getOrElse(contract.sweepScript.toHex),
          sweepConfTarget = contract.sweepConfTarget,
          htlcConfirmations = contract.htlcConfirmations,
          firstHop = contract.firstHop_opt,
          swapFee = swapInvoice.amount_opt.get.truncateToSatoshi + prepayAmount - record.amount,
          prepayAmount = prepayAmount,
          limits = contract.limits,
          publicationDeadline = contract.publicationDeadline,
          htlcOutPoint = swap.progress.htlc_opt.map(o => s"${o.outPoint.txid}:${o.outPoint.index}"),
          htlcAmount = htlcAmount_opt,
          sweepTxIds = swap.progress.sweepTxs.map(_.txid),
          minerFee = for {
            htlcAmount <- htlcAmount_opt
            sweep <- swap.progress.sweepTxs.lastOption if record.state == LoopOut.States.Success
          } yield htlcAmount - sweep.txOut.map(_.amount).sum,
          swapRoutingFee = routingFee(record.swapHash),
          prepayRoutingFee = routingFee(prepayInvoice.paymentHash),
        ))
      case _ => None
    }
    val loopIn_opt = record.swapType match {
      case SwapType.LoopIn =>
        val swap = LoopIn.Swap(record)
        val contract = swap.contract
        val htlcOutput_opt = LoopIn.htlcOutput(swap, keys)
        val swapInvoice = Bolt11Invoice.fromString(contract.swapInvoice).get
        Some(LoopInDetails(
          htlcAddress = LoopIn.htlc(swap, keys).address(chainHash),
          refundAddress = addressFromPublicKeyScript(chainHash, Script.parse(contract.refundScript)).getOrElse(contract.refundScript.toHex),
          htlcConfTarget = contract.htlcConfTarget,
          lastHop = contract.lastHop_opt,
          swapFee = record.amount - swapInvoice.amount_opt.get.truncateToSatoshi,
          limits = contract.limits,
          htlcOutPoint = htlcOutput_opt.map(o => s"${o.outPoint.txid}:${o.outPoint.index}"),
          htlcTxFee = swap.progress.htlcTx_opt.map(_ => swap.progress.htlcTxFee),
          refundTxIds = swap.progress.refundTxs.map(_.txid),
          refundFee = for {
            htlcOutput <- htlcOutput_opt
            refund <- swap.progress.refundTxs.lastOption if record.state == LoopIn.States.FailedTimeout
          } yield htlcOutput.txOut.amount - refund.txOut.map(_.amount).sum,
          amountReceived = payments.incomingStatus(record.swapHash) match {
            case IncomingInvoiceStatus.Received(amount) => Some(amount)
            case _ => None
          },
        ))
      case _ => None
    }
    SwapInfo(record.swapHash, record.swapType.name, record.state, record.amount, record.cltvExpiry, record.createdAt, record.updatedAt, record.requestId_opt.map(_.idempotencyKey), loopOut_opt, loopIn_opt)
  }

  def loopInTerms(): Future[LoopInTerms] = server.loopInTerms()

  /** @param routeHints give the server routing hints for our private channels (see [[LoopInRequest]]). */
  def loopInQuote(amount: Satoshi, lastHop_opt: Option[PublicKey], htlcConfTarget_opt: Option[Int] = None, routeHints: Boolean = false): Future[LoopInQuote] = {
    for {
      hints <- selectRouteHints(routeHints, lastHop_opt)
      quote <- quoteLoopIn(amount, lastHop_opt, htlcConfTarget_opt, hints)
    } yield quote
  }

  private def selectRouteHints(routeHints: Boolean, lastHop_opt: Option[PublicKey]): Future[List[List[ExtraHop]]] =
    if (routeHints) payments.routeHints(lastHop_opt) else Future.successful(Nil)

  private def quoteLoopIn(amount: Satoshi, lastHop_opt: Option[PublicKey], htlcConfTarget_opt: Option[Int], hints: List[List[ExtraHop]]): Future[LoopInQuote] = {
    val htlcConfTarget = htlcConfTarget_opt.getOrElse(LoopIn.DefaultHtlcConfTarget)
    if (htlcConfTarget < MinConfTarget) return fail(s"confirmation target must be at least $MinConfTarget")
    if (htlcConfTarget > MaxConfTarget) return fail(s"confirmation target must be at most $MaxConfTarget")
    for {
      terms <- server.loopInTerms()
      _ <- checkAmount(amount, terms.minSwapAmount, terms.maxSwapAmount).fold(fail, Future.successful)
      // Like loopd, we let the server probe our node first to get a more accurate quote, but we ignore probe failures.
      _ <- server.probe(amount, nodeId, lastHop_opt, hints).recover {
        case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.UNAVAILABLE => ()
        case e =>
          logger.warn(s"server probe error: ${e.getMessage}")
          ()
      }
      quote <- server.loopInQuote(amount, nodeId, lastHop_opt, hints)
    } yield LoopInQuote(amount, quote.swapFee, quote.cltvDelta, htlcConfTarget, LoopInProcessor.htlcFeeEstimate(chain.feerate(htlcConfTarget)))
  }

  /**
   * Creates a loop in swap with the server and starts it: we publish our htlc right away, and the server pays our
   * invoice once it is confirmed.
   */
  def loopIn(request: LoopInRequest): Future[SwapInfo] = {
    val context = s"cannot create loop in amount=${request.amount}"
    withIdempotencyKey(context, request.idempotencyKey_opt, requestHash(request))(createLoopIn(context, request, _))
  }

  private def createLoopIn(context: String, request: LoopInRequest, requestId_opt: Option[SwapRequestId]): Future[SwapInfo] = {
    val prepared = for {
      hints <- selectRouteHints(request.routeHints, request.lastHop_opt)
      quote <- quoteLoopIn(request.amount, request.lastHop_opt, request.htlcConfTarget_opt, hints)
      _ <- checkQuotedSwapFee(request.amount, quote.swapFee, request.maxSwapFee_opt, config).fold(fail, Future.successful)
      refundScript <- request.address_opt match {
        case Some(address) => addressToPublicKeyScript(chainHash, address).map(Script.write).fold(e => fail(s"invalid address: $e"), Future.successful)
        case None => chain.newSweepScript()
      }
    } yield (hints, quote, refundScript)
    // Failures past this point have a swap hash: createLoopIn logs them.
    prepared.transformWith {
      case Failure(t) =>
        logCreationFailure(context, t)
        Future.failed(t)
      case Success((hints, quote, refundScript)) =>
        withKeyIndex(context)(createLoopIn(request, quote, hints, refundScript, requestId_opt, _))
    }
  }

  /** The API returns the reason of a failure to the caller only: we log it too. */
  private def logCreationFailure(context: String, t: Throwable): Unit = t match {
    // Expected failures (invalid requests, swap server errors): their message says it all.
    case _: IllegalArgumentException | _: StatusRuntimeException => logger.warn(s"$context: ${t.getMessage}")
    case _ => logger.error(s"$context: ${t.getMessage}", t)
  }

  private def createLoopIn(request: LoopInRequest, quote: LoopInQuote, hints: List[List[ExtraHop]], refundScript: ByteVector, requestId_opt: Option[SwapRequestId], keyIndex: Long): Future[SwapInfo] = {
    val preimage = randomBytes32()
    val swapHash = Crypto.sha256(preimage)
    val senderKey = keys.swapKey(keyIndex).publicKey
    val senderInternalKey = keys.internalKey(keyIndex, swapHash).publicKey
    val limits = LoopInLimits(
      maxSwapFee = request.maxSwapFee_opt.getOrElse(quote.swapFee),
      maxMinerFee = request.maxMinerFee_opt.getOrElse(quote.htlcPublishFee * LoopInMinerFeeMultiplier),
    )
    val invoiceAmount = (request.amount - quote.swapFee).toMilliSatoshi
    // The server can probe the route to our node with this invoice: we don't know its preimage, so payments fail at
    // our node with a "wrong payment hash" error, which is what the server expects.
    val probeInvoice = payments.unregisteredInvoice(probeHash(swapHash), invoiceAmount, "loop in probe", hints)
    val initiationHeight = currentBlockHeight()
    logger.info(s"creating loop in $swapHash amount=${request.amount} limits=$limits route_hints=${hints.size}")
    payments.createInvoice(preimage, invoiceAmount, "swap", LoopInInvoiceExpiry, hints).flatMap { swapInvoice =>
      server.newLoopInSwap(swapHash, request.amount, senderKey, senderInternalKey, swapInvoice.toString, probeInvoice.toString, request.lastHop_opt).transformWith {
        case Success(response) if validateLoopIn(currentBlockHeight(), response.expiry).isRight =>
          val persisted = Try {
            val contract = LoopInContract(swapInvoice.toString, response.receiverKey, response.receiverInternalKey, request.lastHop_opt, quote.htlcConfTarget, refundScript, limits, initiationHeight)
            val data = LoopInData(contract, LoopInProgress(htlcTx_opt = None, htlcTxFee = 0.sat, keyPushed = false, refundTxs = Nil))
            val now = TimestampMilli.now()
            val record = SwapRecord(swapHash, SwapType.LoopIn, LoopIn.States.Initiated, request.amount, preimage, keyIndex, response.expiry, LoopInData.encode(data), now, now, requestId_opt)
            db.addSwap(record)
            (record, data)
          }
          persisted match {
            case Success((record, data)) =>
              if (response.serverMessage.nonEmpty) logger.info(s"loop in $swapHash: server message: ${response.serverMessage}")
              spawn(LoopIn.Swap(record, data))
              Future.successful(swapInfo(record))
            case Failure(t) =>
              // We won't run this swap: the server must not be able to pay us for it.
              payments.removeInvoice(swapHash)
              Future.failed(t)
          }
        case result =>
          // The server must not be able to pay us for a swap that we won't run.
          payments.removeInvoice(swapHash)
          result match {
            case Success(response) => fail(validateLoopIn(currentBlockHeight(), response.expiry).swap.toOption.get)
            case Failure(t) => Future.failed(t)
          }
      }
    }.andThen { case Failure(t) => logCreationFailure(s"cannot create loop in $swapHash", t) }
  }

  def fetchL402(): Future[Seq[L402TokenInfo]] = server.fetchL402().map(_ => listL402Tokens())

  def listL402Tokens(): Seq[L402TokenInfo] = db.listL402Tokens().map(t => L402TokenInfo(t.paymentHash, t.isPending, t.amountPaid, t.routingFeePaid, t.createdAt))

}
