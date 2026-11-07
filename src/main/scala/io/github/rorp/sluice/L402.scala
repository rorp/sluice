package io.github.rorp.sluice

import akka.util.Timeout
import fr.acinq.bitcoin.scalacompat.{BlockHash, ByteVector32, Crypto, Satoshi}
import fr.acinq.eclair.db.{OutgoingPayment, OutgoingPaymentStatus}
import fr.acinq.eclair.payment.{Bolt11Invoice, PaymentFailed, PaymentSent}
import io.github.rorp.sluice.db.{L402Token, LoopDb}
import fr.acinq.eclair.{EclairImpl, Kit, MilliSatoshi, TimestampMilli}
import grizzled.slf4j.Logging
import io.grpc.{Metadata, Status, StatusRuntimeException}
import scodec.bits.ByteVector

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.{Failure, Success, Try}

/**
 * L402 authentication (formerly LSAT), ported from aperture's l402 package (client_interceptor.go):
 *  - the server answers an unauthenticated call with a gRPC INTERNAL status "payment required" and a
 *    `WWW-Authenticate: L402 macaroon="<base64>", invoice="<bolt11>"` trailer;
 *  - we pay the invoice and append a `preimage=<hex>` first-party caveat to the macaroon;
 *  - every following call carries the hex-encoded macaroon in the `macaroon` request metadata.
 */
object L402 {

  case class Challenge(macaroon: Macaroon, invoice: Bolt11Invoice)

  sealed trait L402Error extends RuntimeException

  case class InvalidChallenge(reason: String) extends RuntimeException(s"invalid L402 challenge: $reason") with L402Error

  case class TokenTooExpensive(amount: MilliSatoshi, maxCost: Satoshi) extends RuntimeException(s"cannot pay for L402 token automatically: cost of $amount exceeds configured max cost of $maxCost") with L402Error

  case class PaymentInFlight(paymentHash: ByteVector32) extends RuntimeException(s"payment of L402 token $paymentHash is still pending, try again later") with L402Error

  case class PaymentFailedError(paymentHash: ByteVector32, reason: String) extends RuntimeException(s"payment of L402 token $paymentHash failed: $reason") with L402Error

  case class TokenRejected(paymentHash: ByteVector32, nextPurchase: TimestampMilli) extends RuntimeException(s"the server rejected our L402 token $paymentHash: we won't pay for a new token automatically before ${Instant.ofEpochMilli(nextPurchase.toLong)}, use loopfetchl402 to pay for one now") with L402Error

  val AuthHeader: Metadata.Key[String] = Metadata.Key.of("www-authenticate", Metadata.ASCII_STRING_MARSHALLER)
  val MacaroonHeader: Metadata.Key[String] = Metadata.Key.of("macaroon", Metadata.ASCII_STRING_MARSHALLER)

  private val authHeaderRegex = "(LSAT|L402) macaroon=\"(.*?)\", invoice=\"(.*?)\"".r

  /** Returns true if the server asked us to pay for an L402 token (see aperture's IsPaymentRequired). */
  def isPaymentRequired(t: Throwable): Boolean = t match {
    case e: StatusRuntimeException =>
      val code = e.getStatus.getCode
      Option(e.getStatus.getDescription).exists(_.toLowerCase.contains("payment required")) && (code == Status.Code.INTERNAL || code == Status.Code.UNKNOWN)
    case _ => false
  }

  /** Parses the first valid challenge found in the WWW-Authenticate trailers. */
  def parseChallenge(trailers: Metadata, chainHash: BlockHash): Try[Challenge] = Try {
    val headers = Option(trailers).flatMap(t => Option(t.getAll(AuthHeader))).map(_.asScala.toSeq).getOrElse(Nil)
    if (headers.isEmpty) throw InvalidChallenge("auth header not found in response")
    val (macBase64, invoiceStr) = headers.flatMap(h => authHeaderRegex.findFirstMatchIn(h)).headOption match {
      case Some(m) => (m.group(2), m.group(3))
      case None => throw InvalidChallenge(s"invalid auth header format: ${headers.head}")
    }
    val macaroon = ByteVector.fromBase64(macBase64).map(Macaroon.decode) match {
      case Some(Success(m)) => m
      case Some(Failure(t)) => throw InvalidChallenge(s"invalid macaroon: ${t.getMessage}")
      case None => throw InvalidChallenge("invalid macaroon base64 encoding")
    }
    val invoice = Bolt11Invoice.fromString(invoiceStr) match {
      case Success(i) => i
      case Failure(t) => throw InvalidChallenge(s"invalid invoice: ${t.getMessage}")
    }
    if (!Bolt11Invoice.prefixes.get(chainHash).contains(invoice.prefix)) throw InvalidChallenge(s"invoice is for another chain (prefix=${invoice.prefix})")
    // The identifier is version (uint16, only version 0 exists) || payment hash || token id (aperture's identifier.go).
    // We check that we are paying for the token we're given.
    if (macaroon.identifier.size != 66 || macaroon.identifier.take(2) != ByteVector(0, 0)) throw InvalidChallenge("unknown macaroon identifier version")
    if (macaroon.identifier.slice(2, 34) != invoice.paymentHash.bytes) throw InvalidChallenge("macaroon payment hash doesn't match invoice payment hash")
    Challenge(macaroon, invoice)
  }

  /** The macaroon to send to the server, with the preimage caveat that proves payment. */
  def paidMacaroon(token: L402Token): Try[Macaroon] = token.preimage_opt match {
    case Some(preimage) => Macaroon.decode(token.baseMacaroon).map(_.addFirstPartyCaveat(ByteVector(s"preimage=${preimage.toHex}".getBytes("US-ASCII"))))
    case None => Failure(new IllegalArgumentException("cannot use a pending L402 token"))
  }

  def credentials(token: L402Token): Try[Metadata] = paidMacaroon(token).map { macaroon =>
    val md = new Metadata()
    md.put(MacaroonHeader, macaroon.encode().toHex)
    md
  }

}

/** Pays and tracks L402 token invoices. */
trait L402Payer {

  import L402Payer._

  def pay(invoice: Bolt11Invoice, maxFee: Satoshi, timeout: FiniteDuration): Future[PaymentResult]

  /** Status of a previous payment, used to resume a pending token after a restart. */
  def status(paymentHash: ByteVector32): PaymentStatus

}

object L402Payer {

  sealed trait PaymentResult

  case class Paid(preimage: ByteVector32, amount: MilliSatoshi, fees: MilliSatoshi) extends PaymentResult

  case class Failed(reason: String) extends PaymentResult

  sealed trait PaymentStatus

  object PaymentStatus {
    case object Unknown extends PaymentStatus

    case object Pending extends PaymentStatus

    case class Succeeded(paid: Paid) extends PaymentStatus

    case object Failed extends PaymentStatus
  }

  /** Labels our L402 payments in eclair's payments db (payments made before the plugin was renamed have eclair-loop-l402). */
  val ExternalId = "sluice-l402"

}

class EclairL402Payer(kit: Kit)(implicit ec: ExecutionContext) extends L402Payer {

  import L402Payer._

  private val eclair = new EclairImpl(kit)

  override def pay(invoice: Bolt11Invoice, maxFee: Satoshi, timeout: FiniteDuration): Future[PaymentResult] = {
    val amount = invoice.amount_opt.getOrElse(throw new IllegalArgumentException("L402 invoice has no amount"))
    eclair.sendBlocking(Some(ExternalId), amount, invoice, maxFeeFlat_opt = Some(maxFee), maxFeePct_opt = Some(0))(Timeout(timeout)).map {
      case s: PaymentSent => Paid(s.paymentPreimage, s.recipientAmount, s.feesPaid)
      case f: PaymentFailed => Failed(f.failures.lastOption.map(_.toString).getOrElse("unknown failure"))
      case other => Failed(s"unexpected payment event: $other")
    }
  }

  override def status(paymentHash: ByteVector32): PaymentStatus = {
    val payments = kit.nodeParams.db.payments.listOutgoingPayments(paymentHash)
    payments.collectFirst { case p@OutgoingPayment(_, _, _, _, _, _, _, _, _, _, _, s: OutgoingPaymentStatus.Succeeded) => (p, s) } match {
      case Some((p, succeeded)) =>
        // A multi-part payment is split in several outgoing payments with the same parent.
        val fees = payments.filter(_.parentId == p.parentId).map(_.status).collect { case s: OutgoingPaymentStatus.Succeeded => s.feesPaid }.sum
        PaymentStatus.Succeeded(Paid(succeeded.paymentPreimage, p.recipientAmount, fees))
      case None if payments.exists(_.status == OutgoingPaymentStatus.Pending) => PaymentStatus.Pending
      case None if payments.nonEmpty => PaymentStatus.Failed
      case None => PaymentStatus.Unknown
    }
  }

}

/**
 * Attaches L402 credentials to swap server calls, and acquires a token when the server asks for one.
 * Token acquisition is serialized so that concurrent calls never pay for two tokens.
 *
 * @param minPurchaseInterval when the server rejects our token, we only pay for a new one automatically if we bought
 *                            ours more than this ago: otherwise the server could make us pay for a token on every call
 *                            (our swaps retry their calls every block and every minute).
 */
class L402Authenticator(db: LoopDb, payer: L402Payer, chainHash: BlockHash, maxCost: Satoshi, maxRoutingFee: Satoshi, paymentTimeout: FiniteDuration, minPurchaseInterval: FiniteDuration)(implicit ec: ExecutionContext) extends Logging {

  import L402._
  import L402Payer._

  private var pendingAcquisition_opt: Option[Future[L402Token]] = None

  private def paidToken_opt(): Option[L402Token] = db.currentL402Token().filterNot(_.isPending)

  /**
   * Runs an rpc with our current credentials (if any). If the server asks for a payment, we pay for a token and
   * retry once with the new credentials.
   *
   * @param forcePurchase pay for a new token if the server rejects ours, even if we bought it recently.
   */
  def authenticated[T](rpc: Metadata => Future[T], forcePurchase: Boolean = false): Future[T] = {
    val token_opt = paidToken_opt()
    val md = token_opt.map(t => credentials(t).get).getOrElse(new Metadata())
    rpc(md).recoverWith {
      case e: StatusRuntimeException if isPaymentRequired(e) =>
        for {
          token <- acquireToken(Option(e.getTrailers).getOrElse(Status.trailersFromThrowable(e)), rejected_opt = token_opt, forcePurchase)
          res <- rpc(credentials(token).get)
        } yield res
    }
  }

  private def acquireToken(trailers: Metadata, rejected_opt: Option[L402Token], forcePurchase: Boolean): Future[L402Token] = synchronized {
    pendingAcquisition_opt match {
      case Some(f) if !f.isCompleted => f
      case _ =>
        // A concurrent call may have acquired a new token since we started our call.
        paidToken_opt() match {
          case Some(token) if !rejected_opt.map(_.paymentHash).contains(token.paymentHash) => Future.successful(token)
          case Some(token) if !forcePurchase && TimestampMilli.now() < token.createdAt + minPurchaseInterval =>
            logger.warn(s"the server rejected our L402 token ${token.paymentHash}, bought at ${Instant.ofEpochMilli(token.createdAt.toLong)}")
            Future.failed(TokenRejected(token.paymentHash, token.createdAt + minPurchaseInterval))
          case _ =>
            val f = Future.fromTry(Try(resumeOrPay(trailers))).flatten
            pendingAcquisition_opt = Some(f)
            f
        }
    }
  }

  private def resumeOrPay(trailers: Metadata): Future[L402Token] = {
    db.currentL402Token() match {
      case Some(pending) if pending.isPending =>
        payer.status(pending.paymentHash) match {
          case PaymentStatus.Succeeded(paid) =>
            logger.info(s"payment of pending L402 token ${pending.paymentHash} succeeded")
            Future.successful(storePaid(pending, paid))
          case PaymentStatus.Pending =>
            Future.failed(PaymentInFlight(pending.paymentHash))
          case PaymentStatus.Failed | PaymentStatus.Unknown =>
            logger.info(s"payment of pending L402 token ${pending.paymentHash} failed, paying for a new token")
            db.removePendingL402Token(pending.paymentHash)
            payNewToken(trailers)
        }
      case _ => payNewToken(trailers)
    }
  }

  private def payNewToken(trailers: Metadata): Future[L402Token] = {
    val challenge = parseChallenge(trailers, chainHash).get
    val amount = challenge.invoice.amount_opt.getOrElse(throw InvalidChallenge("invoice has no amount"))
    if (amount > maxCost) throw TokenTooExpensive(amount, maxCost)
    // We store the pending token before paying, so that we can resume if we're interrupted.
    val pending = L402Token(challenge.invoice.paymentHash, challenge.macaroon.encode(), None, MilliSatoshi(0), MilliSatoshi(0), TimestampMilli.now())
    db.storeL402Token(pending)
    logger.info(s"paying $amount for L402 token ${pending.paymentHash}")
    payer.pay(challenge.invoice, maxRoutingFee, paymentTimeout).map {
      case paid: Paid if Crypto.sha256(paid.preimage) == pending.paymentHash =>
        logger.info(s"paid L402 token ${pending.paymentHash} (amount=${paid.amount} fees=${paid.fees})")
        storePaid(pending, paid)
      case paid: Paid =>
        throw PaymentFailedError(pending.paymentHash, s"invalid preimage ${paid.preimage}")
      case Failed(reason) =>
        db.removePendingL402Token(pending.paymentHash)
        throw PaymentFailedError(pending.paymentHash, reason)
    }
    // NB: if the payment times out, we keep the pending token and will check its status on the next attempt.
  }

  private def storePaid(pending: L402Token, paid: Paid): L402Token = {
    val token = pending.copy(preimage_opt = Some(paid.preimage), amountPaid = paid.amount, routingFeePaid = paid.fees)
    db.storeL402Token(token)
    token
  }

}
