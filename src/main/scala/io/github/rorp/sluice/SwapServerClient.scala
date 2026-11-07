package io.github.rorp.sluice

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, MoreExecutors}
import com.google.protobuf.ByteString
import fr.acinq.bitcoin.scalacompat.Crypto.{PrivateKey, PublicKey}
import fr.acinq.bitcoin.psbt.Psbt
import fr.acinq.bitcoin.scalacompat.Musig2.IndividualNonce
import fr.acinq.bitcoin.scalacompat.{ByteVector32, KotlinUtils, OutPoint, Satoshi, SatoshiLong, Transaction, TxOut}
import fr.acinq.eclair.BlockHeight
import fr.acinq.eclair.payment.Bolt11Invoice.ExtraHop
import io.github.rorp.sluice.swapserverrpc._
import grizzled.slf4j.Logging
import io.grpc.ManagedChannel
import io.grpc.netty.shaded.io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.grpc.stub.MetadataUtils
import scodec.bits.ByteVector

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Try

/**
 * Client for the Loop swap server (swapserverrpc/server.proto), ported from loop's swap_server_client.go.
 * Every call is authenticated with L402.
 */
class SwapServerClient(channel: ManagedChannel, l402: L402Authenticator, userAgent: String, rpcTimeout: FiniteDuration)(implicit ec: ExecutionContext) extends Logging {

  import SwapServerClient._

  private val stub = SwapServerGrpc.newFutureStub(channel)

  private def call[Resp](rpc: SwapServerGrpc.SwapServerFutureStub => ListenableFuture[Resp], forcePurchase: Boolean = false): Future[Resp] =
    l402.authenticated(md => {
      val s = stub
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md))
        .withDeadlineAfter(rpcTimeout.toMillis, TimeUnit.MILLISECONDS)
      toScala(rpc(s))
    }, forcePurchase)

  def loopOutTerms(): Future[LoopOutTerms] = {
    val req = ServerLoopOutTermsRequest.newBuilder()
      .setProtocolVersion(CurrentProtocolVersion)
      .setUserAgent(userAgent)
      .build()
    call(_.loopOutTerms(req)).map(r => LoopOutTerms(r.getMinSwapAmount.sat, r.getMaxSwapAmount.sat, r.getMinCltvDelta, r.getMaxCltvDelta))
  }

  /**
   * @param expiry                  absolute expiry height of the on-chain htlc we would request.
   * @param swapPublicationDeadline unix time (seconds) by which the server must publish the on-chain htlc.
   */
  def loopOutQuote(amount: Satoshi, expiry: BlockHeight, swapPublicationDeadline: Long): Future[LoopOutServerQuote] = {
    val req = ServerLoopOutQuoteRequest.newBuilder()
      .setAmt(amount.toLong)
      .setSwapPublicationDeadline(swapPublicationDeadline)
      .setProtocolVersion(CurrentProtocolVersion)
      .setExpiry(expiry.toLong.toInt)
      .setUserAgent(userAgent)
      .build()
    call(_.loopOutQuote(req)).map { r =>
      val dest = ByteVector.fromHex(r.getSwapPaymentDest).filter(_.size == 33).flatMap(b => Try(PublicKey(b)).toOption).getOrElse(throw new IllegalArgumentException("invalid payment dest"))
      LoopOutServerQuote(swapFee = r.getSwapFee.sat, prepayAmount = r.getPrepayAmt.sat, swapPaymentDest = dest)
    }
  }

  def loopInTerms(): Future[LoopInTerms] = {
    val req = ServerLoopInTermsRequest.newBuilder()
      .setProtocolVersion(CurrentProtocolVersion)
      .setUserAgent(userAgent)
      .build()
    call(_.loopInTerms(req)).map(r => LoopInTerms(r.getMinSwapAmount.sat, r.getMaxSwapAmount.sat))
  }

  /** @param routeHints routing hints to our node, which the server uses to compute the routing fee it pays. */
  def loopInQuote(amount: Satoshi, nodeId: PublicKey, lastHop_opt: Option[PublicKey], routeHints: List[List[ExtraHop]] = Nil): Future[LoopInServerQuote] = {
    val builder = ServerLoopInQuoteRequest.newBuilder()
      .setAmt(amount.toLong)
      .setProtocolVersion(CurrentProtocolVersion)
      .setPubkey(bytes(nodeId.value))
      .setUserAgent(userAgent)
      .addAllRouteHints(routeHintsProto(routeHints))
    lastHop_opt.foreach(n => builder.setLastHop(bytes(n.value)))
    call(_.loopInQuote(builder.build())).map(r => LoopInServerQuote(swapFee = r.getSwapFee.sat, cltvDelta = r.getCltvDelta))
  }

  /**
   * Asks the server to probe the route to our node, so that it can give us a more accurate loop in quote.
   *
   * @param routeHints routing hints to our node, so that the server can probe through our private channels.
   */
  def probe(amount: Satoshi, nodeId: PublicKey, lastHop_opt: Option[PublicKey], routeHints: List[List[ExtraHop]] = Nil): Future[Unit] = {
    val builder = ServerProbeRequest.newBuilder()
      .setProtocolVersion(CurrentProtocolVersion)
      .setAmt(amount.toLong)
      .setTarget(bytes(nodeId.value))
      .addAllRouteHints(routeHintsProto(routeHints))
    lastHop_opt.foreach(n => builder.setLastHop(bytes(n.value)))
    call(_.probe(builder.build())).map(_ => ())
  }

  /**
   * Creates a loop out swap on the server, which returns the invoices we must pay and its htlc key.
   *
   * @param expiry absolute expiry height of the on-chain htlc we request.
   */
  def newLoopOutSwap(swapHash: ByteVector32, amount: Satoshi, receiverKey: PublicKey, expiry: BlockHeight, swapPublicationDeadline: Long): Future[NewLoopOutSwapResponse] = {
    val req = ServerLoopOutRequest.newBuilder()
      .setReceiverKey(bytes(receiverKey.value))
      .setSwapHash(bytes(swapHash))
      .setAmt(amount.toLong)
      .setSwapPublicationDeadline(swapPublicationDeadline)
      .setProtocolVersion(CurrentProtocolVersion)
      .setExpiry(expiry.toLong.toInt)
      .setUserAgent(userAgent)
      .build()
    call(_.newLoopOutSwap(req)).map { r =>
      val senderKey = Try(PublicKey(ByteVector(r.getSenderKey.toByteArray))).getOrElse(throw new IllegalArgumentException("invalid server key"))
      NewLoopOutSwapResponse(r.getSwapInvoice, r.getPrepayInvoice, senderKey, r.getServerMessage)
    }
  }

  /**
   * Tells the server that we gave up on a loop out because one of our payments failed, and why (like loopd).
   *
   * @param prepay whether the failed payment is the prepay (otherwise the swap payment).
   */
  def cancelLoopOutSwap(swapHash: ByteVector32, paymentAddress: ByteVector32, prepay: Boolean, failure: PaymentFailureInfo): Future[Unit] = {
    val routeCancel = RouteCancel.newBuilder()
      .setRouteType(if (prepay) RoutePaymentType.PREPAY_ROUTE else RoutePaymentType.INVOICE_ROUTE)
      .setFailure(failure.reason)
    failure.remainingHops.foreach(h => routeCancel.addAttempts(HtlcAttempt.newBuilder().setRemainingHops(h.toInt).build()))
    val req = CancelLoopOutSwapRequest.newBuilder()
      .setProtocolVersion(CurrentProtocolVersion)
      .setSwapHash(bytes(swapHash))
      .setPaymentAddress(bytes(paymentAddress))
      .setRouteCancel(routeCancel.build())
      .build()
    call(_.cancelLoopOutSwap(req)).map(_ => ())
  }

  /** Gives our loop out preimage to the server, so that it can settle our swap payment without waiting for our sweep. */
  def loopOutPushPreimage(preimage: ByteVector32): Future[Unit] = {
    val req = ServerLoopOutPushPreimageRequest.newBuilder()
      .setProtocolVersion(CurrentProtocolVersion)
      .setPreimage(bytes(preimage))
      .build()
    call(_.loopOutPushPreimage(req)).map(_ => ())
  }

  /**
   * Asks the server to co-sign the key path spend of a loop out htlc (MuSig2).
   *
   * @param paymentAddress payment secret of the swap invoice, proves that we're the owner of the swap.
   * @param nonce          our public MuSig2 nonce for this signing session.
   * @param sweepTx        unsigned sweep transaction.
   * @param prevOuts       outputs spent by the sweep transaction.
   */
  def muSig2SignSweep(swapHash: ByteVector32, paymentAddress: ByteVector32, nonce: IndividualNonce, sweepTx: Transaction, prevOuts: Map[OutPoint, TxOut]): Future[MuSig2SignSweepResponse] = {
    val builder = MuSig2SignSweepReq.newBuilder()
      .setProtocolVersion(CurrentProtocolVersion)
      .setSwapHash(bytes(swapHash))
      .setPaymentAddress(bytes(paymentAddress))
      .setNonce(bytes(nonce.data))
      .setSweepTxPsbt(bytes(SwapServerClient.sweepPsbt(sweepTx, prevOuts)))
    prevOuts.foreach { case (outPoint, txOut) =>
      builder.addPrevoutInfo(PrevoutInfo.newBuilder()
        .setValue(txOut.amount.toLong)
        .setPkScript(bytes(txOut.publicKeyScript))
        // txid in internal byte order, like go's chainhash.Hash.
        .setTxidBytes(bytes(outPoint.hash.value))
        .setOutputIndex(outPoint.index.toInt)
        .build())
    }
    call(_.muSig2SignSweep(builder.build())).map { r =>
      val nonce = Try(IndividualNonce(ByteVector(r.getNonce.toByteArray))).filter(_.data.size == 66).getOrElse(throw new IllegalArgumentException("invalid server nonce"))
      val sig = Some(ByteVector(r.getPartialSignature.toByteArray)).filter(_.size == 32).map(ByteVector32(_)).getOrElse(throw new IllegalArgumentException("invalid server partial signature"))
      MuSig2SignSweepResponse(nonce, sig)
    }
  }

  /**
   * Creates a loop in swap on the server: it will pay our swap invoice once our htlc is confirmed.
   *
   * @param senderKey         our htlc key (timeout path).
   * @param senderInternalKey our MuSig2 internal key.
   * @param swapInvoice       the invoice the server will pay (swap amount minus swap fee).
   * @param probeInvoice      an invoice the server can use to probe the route to us, which we never accept.
   */
  def newLoopInSwap(swapHash: ByteVector32, amount: Satoshi, senderKey: PublicKey, senderInternalKey: PublicKey, swapInvoice: String, probeInvoice: String, lastHop_opt: Option[PublicKey]): Future[NewLoopInSwapResponse] = {
    val builder = ServerLoopInRequest.newBuilder()
      .setSenderKey(bytes(senderKey.value))
      .setSenderInternalPubkey(bytes(senderInternalKey.value))
      .setSwapHash(bytes(swapHash))
      .setAmt(amount.toLong)
      .setSwapInvoice(swapInvoice)
      .setProtocolVersion(CurrentProtocolVersion)
      .setProbeInvoice(probeInvoice)
      .setUserAgent(userAgent)
    lastHop_opt.foreach(n => builder.setLastHop(bytes(n.value)))
    call(_.newLoopInSwap(builder.build())).map { r =>
      def key(b: ByteString, name: String): PublicKey = Try(PublicKey(ByteVector(b.toByteArray))).getOrElse(throw new IllegalArgumentException(s"invalid server $name"))

      NewLoopInSwapResponse(key(r.getReceiverKey, "key"), key(r.getReceiverInternalPubkey, "internal key"), BlockHeight(r.getExpiry), r.getServerMessage)
    }
  }

  /** Reveals our MuSig2 internal key once our invoice has been paid, so that the server can sweep through the key path. */
  def pushKey(swapHash: ByteVector32, internalKey: PrivateKey): Future[Unit] = {
    val req = ServerPushKeyReq.newBuilder()
      .setProtocolVersion(CurrentProtocolVersion)
      .setSwapHash(bytes(swapHash))
      .setInternalPrivkey(bytes(internalKey.value))
      .build()
    call(_.pushKey(req)).map(_ => ())
  }

  /**
   * Makes sure that we have an L402 token that the server accepts: we pay for a new one if the server rejects ours, even
   * if we bought it recently (this call isn't free then).
   */
  def fetchL402(): Future[Unit] = call(_.fetchL402(FetchL402Request.newBuilder().build()), forcePurchase = true).map(_ => ())

  def close(): Unit = {
    channel.shutdown()
    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) channel.shutdownNow()
  }

}

object SwapServerClient {

  /** taproot htlcs (v3) with musig2 cooperative spends, see loopdb/protocol_version.go. */
  val CurrentProtocolVersion: ProtocolVersion = ProtocolVersion.MUSIG2

  /**
   * Serialized psbt of a sweep for MuSig2SignSweep. Like loopd, we include the outputs spent by each input
   * (witness_utxo): the server rejects sweeps without them, even though they're also in the request's prevout_info.
   */
  def sweepPsbt(sweepTx: Transaction, prevOuts: Map[OutPoint, TxOut]): ByteVector = {
    val psbt = sweepTx.txIn.foldLeft(new Psbt(KotlinUtils.scala2kmp(sweepTx))) { case (psbt, txIn) =>
      val updated = psbt.updateWitnessInput(KotlinUtils.scala2kmp(txIn.outPoint), KotlinUtils.scala2kmp(prevOuts(txIn.outPoint)), null, null, null, java.util.Map.of(), null, null, java.util.Map.of())
      if (updated.isRight) updated.getRight else throw new IllegalArgumentException(s"cannot add spent output to sweep psbt: ${updated.getLeft}")
    }
    KotlinUtils.kmp2scala(Psbt.write(psbt))
  }

  case class LoopOutTerms(minSwapAmount: Satoshi, maxSwapAmount: Satoshi, minCltvDelta: Int, maxCltvDelta: Int)

  case class LoopOutServerQuote(swapFee: Satoshi, prepayAmount: Satoshi, swapPaymentDest: PublicKey)

  case class LoopInTerms(minSwapAmount: Satoshi, maxSwapAmount: Satoshi)

  case class LoopInServerQuote(swapFee: Satoshi, cltvDelta: Int)

  case class NewLoopOutSwapResponse(swapInvoice: String, prepayInvoice: String, serverKey: PublicKey, serverMessage: String)

  case class NewLoopInSwapResponse(receiverKey: PublicKey, receiverInternalKey: PublicKey, expiry: BlockHeight, serverMessage: String)

  case class MuSig2SignSweepResponse(serverNonce: IndividualNonce, partialSig: ByteVector32)

  private def bytes(b: ByteVector): ByteString = ByteString.copyFrom(b.toArray)

  /** Like loopd's marshallRouteHints. */
  def routeHintsProto(routeHints: List[List[ExtraHop]]): java.util.List[RouteHint] = routeHints.map { hops =>
    RouteHint.newBuilder().addAllHopHints(hops.map { hop =>
      HopHint.newBuilder()
        .setNodeId(hop.nodeId.toHex)
        .setChanId(hop.shortChannelId.toLong)
        .setFeeBaseMsat(hop.feeBase.toLong.toInt)
        .setFeeProportionalMillionths(hop.feeProportionalMillionths.toInt)
        .setCltvExpiryDelta(hop.cltvExpiryDelta.toInt)
        .build()
    }.asJava).build()
  }.asJava

  def toScala[T](lf: ListenableFuture[T]): Future[T] = {
    val p = Promise[T]()
    Futures.addCallback(lf, new FutureCallback[T] {
      override def onSuccess(result: T): Unit = p.success(result)

      override def onFailure(t: Throwable): Unit = p.failure(t)
    }, MoreExecutors.directExecutor())
    p.future
  }

  def createChannel(config: SluiceConfig): ManagedChannel = {
    val builder = NettyChannelBuilder.forTarget(config.serverAddress)
    config.tls match {
      case ServerTls.SystemCa => builder.useTransportSecurity()
      case ServerTls.TrustedCert(file) => builder.sslContext(GrpcSslContexts.forClient().trustManager(file).build())
      case ServerTls.Insecure => builder.usePlaintext()
    }
    builder.userAgent(config.userAgent).build()
  }

  def apply(config: SluiceConfig, l402: L402Authenticator)(implicit ec: ExecutionContext): SwapServerClient =
    new SwapServerClient(createChannel(config), l402, config.userAgent, config.rpcTimeout)

}
