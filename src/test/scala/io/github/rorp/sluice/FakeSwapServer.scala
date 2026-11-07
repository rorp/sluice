package io.github.rorp.sluice

import com.google.protobuf.ByteString
import fr.acinq.bitcoin.psbt.Psbt
import fr.acinq.bitcoin.scalacompat.Crypto.{PrivateKey, PublicKey}
import fr.acinq.bitcoin.scalacompat.Musig2.IndividualNonce
import fr.acinq.bitcoin.scalacompat.{Block, ByteVector32, Crypto, KotlinUtils, Musig2, OutPoint, Satoshi, TxHash, TxOut}
import fr.acinq.eclair.payment.Bolt11Invoice
import io.github.rorp.sluice.L402Payer.{Paid, PaymentResult, PaymentStatus}
import io.github.rorp.sluice.swapserverrpc._
import fr.acinq.eclair.{BlockHeight, CltvExpiryDelta, MilliSatoshi, ToMilliSatoshiConversion, randomBytes32, randomKey}
import io.grpc._
import io.grpc.stub.StreamObserver
import scodec.bits.ByteVector

import java.util.Base64
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, ListHasAsScala}

/**
 * A fake swap server protected by a fake aperture proxy issuing L402 tokens, for tests.
 */
class FakeSwapServer(val l402Price: MilliSatoshi = MilliSatoshi(100_000)) {

  private val rootKey = randomBytes32().bytes
  private val serverKey: PrivateKey = randomKey()
  val serverNodeId = serverKey.publicKey
  val swapPaymentDest = randomKey().publicKey

  /** preimages of the invoices we issued, so that the fake payer can "pay" them. */
  val preimages = TrieMap.empty[ByteVector32, ByteVector32]
  val revoked = TrieMap.empty[ByteVector32, Unit]
  val challengesIssued = new AtomicInteger(0)
  val requests = new ConcurrentLinkedQueue[AnyRef]()
  @volatile var probeStatus: Option[Status] = None

  /** A loop out swap created on this server. */
  case class FakeLoopOut(swapHash: ByteVector32, amount: Satoshi, htlcKey: PrivateKey, htlc: Htlc, swapInvoice: Bolt11Invoice, prepayInvoice: Bolt11Invoice)

  val loopOuts = TrieMap.empty[ByteVector32, FakeLoopOut]
  val pushedPreimages = TrieMap.empty[ByteVector32, ByteVector32]
  /** We fail preimage pushes with this status when set. */
  @volatile var pushPreimageFailure_opt: Option[Status] = None
  /** All calls fail when this is set (the server is down). */
  @volatile var unavailable: Boolean = false
  /** Whether we co-sign loop out sweeps. */
  @volatile var cooperativeSweeps: Boolean = true
  /** Lets tests tamper with the invoices we return. */
  @volatile var loopOutSwapFee: Satoshi => Satoshi = amount => amount / 100
  @volatile var loopOutPrepay: Satoshi = Satoshi(1337)
  /** min and max loop out cltv deltas. */
  @volatile var loopOutCltvDeltas: (Int, Int) = (20, 60)
  /** We answer NewLoopOutSwap once this completes (a slow server). */
  @volatile var newLoopOutSwapReply: Future[Unit] = Future.unit

  /** A loop in swap created on this server. */
  case class FakeLoopIn(swapHash: ByteVector32, amount: Satoshi, htlcKey: PrivateKey, internalKey: PrivateKey, htlc: Htlc, swapInvoice: Bolt11Invoice, probeInvoice: Bolt11Invoice)

  val loopIns = TrieMap.empty[ByteVector32, FakeLoopIn]
  /** Client internal keys pushed after we paid their loop in invoice. */
  val pushedKeys = TrieMap.empty[ByteVector32, PrivateKey]
  /** Expiry of new loop in htlcs (the server doesn't know the current block height). */
  @volatile var loopInExpiry: BlockHeight = BlockHeight(801_000)

  def requestsOfType[T](implicit ct: scala.reflect.ClassTag[T]): Seq[T] = requests.asScala.toSeq.collect { case r: T => r }

  private def keyedHash(key: ByteVector, data: ByteVector): ByteVector32 = Macaroon.hmac256(key, data)

  private def newChallenge(): Metadata = {
    val preimage = randomBytes32()
    val paymentHash = Crypto.sha256(preimage)
    preimages.put(paymentHash, preimage)
    val id = ByteVector(0, 0) ++ paymentHash.bytes ++ randomBytes32().bytes
    val sig = keyedHash(keyedHash(ByteVector("macaroons-key-generator".getBytes), rootKey).bytes, id)
    val macaroon = Macaroon(Some(ByteVector("lsat".getBytes)), id, Nil, sig)
    val invoice = Bolt11Invoice(Block.RegtestGenesisBlock.hash, Some(l402Price), paymentHash, serverKey, Left("l402"), CltvExpiryDelta(18))
    val macBase64 = Base64.getEncoder.encodeToString(macaroon.encode().toArray)
    val trailers = new Metadata()
    trailers.put(L402.AuthHeader, s"""LSAT macaroon="$macBase64", invoice="${invoice.toString}"""")
    trailers.put(L402.AuthHeader, s"""L402 macaroon="$macBase64", invoice="${invoice.toString}"""")
    challengesIssued.incrementAndGet()
    trailers
  }

  /** Verifies the macaroon like aperture would: signature chain and preimage caveat. */
  private def isAuthenticated(headers: Metadata): Boolean = {
    Option(headers.get(L402.MacaroonHeader)).flatMap(ByteVector.fromHex(_)).flatMap(Macaroon.decode(_).toOption).exists { m =>
      val sig0 = keyedHash(keyedHash(ByteVector("macaroons-key-generator".getBytes), rootKey).bytes, m.identifier)
      val sig = m.caveats.foldLeft(sig0) { case (s, c) => keyedHash(s.bytes, c.identifier) }
      val paymentHash = ByteVector32(m.identifier.slice(2, 34))
      val preimageOk = m.caveats.map(c => new String(c.identifier.toArray)).collectFirst {
        case s if s.startsWith("preimage=") => ByteVector32.fromValidHex(s.stripPrefix("preimage="))
      }.exists(p => Crypto.sha256(p) == paymentHash)
      sig == m.signature && preimageOk && !revoked.contains(paymentHash)
    }
  }

  private val aperture = new ServerInterceptor {
    override def interceptCall[ReqT, RespT](call: ServerCall[ReqT, RespT], headers: Metadata, next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
      if (unavailable) {
        call.close(Status.UNAVAILABLE.withDescription("server is down"), new Metadata())
        new ServerCall.Listener[ReqT] {}
      } else if (isAuthenticated(headers)) {
        next.startCall(call, headers)
      } else {
        call.close(Status.INTERNAL.withDescription("payment required"), newChallenge())
        new ServerCall.Listener[ReqT] {}
      }
    }
  }

  private def reply[T](observer: StreamObserver[T], value: T): Unit = {
    observer.onNext(value)
    observer.onCompleted()
  }

  private val service = new SwapServerGrpc.SwapServerImplBase {
    override def loopOutTerms(request: ServerLoopOutTermsRequest, observer: StreamObserver[ServerLoopOutTerms]): Unit = {
      requests.add(request)
      reply(observer, ServerLoopOutTerms.newBuilder().setMinSwapAmount(250_000).setMaxSwapAmount(5_000_000).setMinCltvDelta(loopOutCltvDeltas._1).setMaxCltvDelta(loopOutCltvDeltas._2).build())
    }

    override def loopOutQuote(request: ServerLoopOutQuoteRequest, observer: StreamObserver[ServerLoopOutQuote]): Unit = {
      requests.add(request)
      reply(observer, ServerLoopOutQuote.newBuilder().setSwapPaymentDest(swapPaymentDest.toHex).setSwapFee(request.getAmt / 100).setPrepayAmt(1337).build())
    }

    override def loopInTerms(request: ServerLoopInTermsRequest, observer: StreamObserver[ServerLoopInTerms]): Unit = {
      requests.add(request)
      reply(observer, ServerLoopInTerms.newBuilder().setMinSwapAmount(250_000).setMaxSwapAmount(2_000_000).build())
    }

    override def loopInQuote(request: ServerLoopInQuoteRequest, observer: StreamObserver[ServerLoopInQuoteResponse]): Unit = {
      requests.add(request)
      reply(observer, ServerLoopInQuoteResponse.newBuilder().setSwapFee(request.getAmt / 200).setCltvDelta(1000).build())
    }

    override def probe(request: ServerProbeRequest, observer: StreamObserver[ServerProbeResponse]): Unit = {
      requests.add(request)
      probeStatus match {
        case Some(status) => observer.onError(status.asRuntimeException())
        case None => reply(observer, ServerProbeResponse.newBuilder().build())
      }
    }

    override def newLoopOutSwap(request: ServerLoopOutRequest, observer: StreamObserver[ServerLoopOutResponse]): Unit = {
      requests.add(request)
      val swapHash = ByteVector32(ByteVector(request.getSwapHash.toByteArray))
      val amount = Satoshi(request.getAmt)
      val receiverKey = PublicKey(ByteVector(request.getReceiverKey.toByteArray))
      val htlcKey = randomKey()
      val htlc = Htlc(swapHash, BlockHeight(request.getExpiry), htlcKey.publicKey, receiverKey, htlcKey.publicKey, receiverKey)
      val prepayPreimage = randomBytes32()
      preimages.put(Crypto.sha256(prepayPreimage), prepayPreimage)
      val swapInvoice = Bolt11Invoice(Block.RegtestGenesisBlock.hash, Some((amount + loopOutSwapFee(amount) - loopOutPrepay).toMilliSatoshi), swapHash, serverKey, Left("swap"), CltvExpiryDelta(200))
      val prepayInvoice = Bolt11Invoice(Block.RegtestGenesisBlock.hash, Some(loopOutPrepay.toMilliSatoshi), Crypto.sha256(prepayPreimage), serverKey, Left("prepay"), CltvExpiryDelta(18))
      loopOuts.put(swapHash, FakeLoopOut(swapHash, amount, htlcKey, htlc, swapInvoice, prepayInvoice))
      val response = ServerLoopOutResponse.newBuilder()
        .setSwapInvoice(swapInvoice.toString)
        .setPrepayInvoice(prepayInvoice.toString)
        .setSenderKey(ByteString.copyFrom(htlcKey.publicKey.value.toArray))
        .build()
      if (newLoopOutSwapReply.isCompleted) reply(observer, response)
      else newLoopOutSwapReply.foreach(_ => reply(observer, response))(ExecutionContext.global)
    }

    override def loopOutPushPreimage(request: ServerLoopOutPushPreimageRequest, observer: StreamObserver[ServerLoopOutPushPreimageResponse]): Unit = {
      requests.add(request)
      pushPreimageFailure_opt match {
        case Some(status) => observer.onError(status.asRuntimeException())
        case None =>
          val preimage = ByteVector32(ByteVector(request.getPreimage.toByteArray))
          pushedPreimages.put(Crypto.sha256(preimage), preimage)
          reply(observer, ServerLoopOutPushPreimageResponse.newBuilder().build())
      }
    }

    override def muSig2SignSweep(request: MuSig2SignSweepReq, observer: StreamObserver[MuSig2SignSweepRes]): Unit = {
      requests.add(request)
      val swapHash = ByteVector32(ByteVector(request.getSwapHash.toByteArray))
      val psbt = Psbt.read(request.getSweepTxPsbt.toByteArray)
      loopOuts.get(swapHash) match {
        case Some(swap) if psbt.isRight && cooperativeSweeps && pushedPreimages.contains(swapHash) && ByteVector(request.getPaymentAddress.toByteArray) == swap.swapInvoice.paymentSecret.bytes =>
          val tx = KotlinUtils.kmp2scala(psbt.getRight.global.tx)
          val prevOuts = request.getPrevoutInfoList.asScala.map(p => OutPoint(TxHash(ByteVector32(ByteVector(p.getTxidBytes.toByteArray))), p.getOutputIndex) -> TxOut(Satoshi(p.getValue), ByteVector(p.getPkScript.toByteArray))).toMap
          val spent = tx.txIn.map(i => prevOuts(i.outPoint))
          // Like loop's server, we require the psbt to include the spent outputs (loopd always sets them).
          val witnessUtxos = psbt.getRight.inputs.asScala.toSeq.map(i => Option(i.getWitnessUtxo).map(KotlinUtils.kmp2scala))
          if (witnessUtxos != spent.map(Some(_))) {
            observer.onError(Status.UNKNOWN.withDescription("invalid sweep").asRuntimeException())
            return
          }
          val inputIndex = spent.indexWhere(_.publicKeyScript == swap.htlc.pubkeyScriptBytes)
          val clientNonce = IndividualNonce(ByteVector(request.getNonce.toByteArray))
          val serverNonce = Musig2.generateNonce(randomBytes32(), Left(swap.htlcKey), swap.htlc.internalKeys, None, None)
          val sig = Musig2.signTaprootInput(swap.htlcKey, tx, inputIndex, spent, swap.htlc.internalKeys, serverNonce.secretNonce, Seq(clientNonce, serverNonce.publicNonce), Some(swap.htlc.scriptTree)).toOption.get
          reply(observer, MuSig2SignSweepRes.newBuilder()
            .setNonce(ByteString.copyFrom(serverNonce.publicNonce.data.toArray))
            .setPartialSignature(ByteString.copyFrom(sig.toArray))
            .build())
        case _ => observer.onError(Status.FAILED_PRECONDITION.withDescription("cannot sign sweep").asRuntimeException())
      }
    }

    override def cancelLoopOutSwap(request: CancelLoopOutSwapRequest, observer: StreamObserver[CancelLoopOutSwapResponse]): Unit = {
      requests.add(request)
      reply(observer, CancelLoopOutSwapResponse.newBuilder().build())
    }

    override def newLoopInSwap(request: ServerLoopInRequest, observer: StreamObserver[ServerLoopInResponse]): Unit = {
      requests.add(request)
      val swapHash = ByteVector32(ByteVector(request.getSwapHash.toByteArray))
      val senderKey = PublicKey(ByteVector(request.getSenderKey.toByteArray))
      val senderInternalKey = PublicKey(ByteVector(request.getSenderInternalPubkey.toByteArray))
      val (htlcKey, internalKey) = (randomKey(), randomKey())
      val htlc = Htlc(swapHash, loopInExpiry, senderInternalKey, internalKey.publicKey, senderKey, htlcKey.publicKey)
      val swapInvoice = Bolt11Invoice.fromString(request.getSwapInvoice).get
      val probeInvoice = Bolt11Invoice.fromString(request.getProbeInvoice).get
      loopIns.put(swapHash, FakeLoopIn(swapHash, Satoshi(request.getAmt), htlcKey, internalKey, htlc, swapInvoice, probeInvoice))
      reply(observer, ServerLoopInResponse.newBuilder()
        .setReceiverKey(ByteString.copyFrom(htlcKey.publicKey.value.toArray))
        .setReceiverInternalPubkey(ByteString.copyFrom(internalKey.publicKey.value.toArray))
        .setExpiry(loopInExpiry.toInt)
        .build())
    }

    override def pushKey(request: ServerPushKeyReq, observer: StreamObserver[ServerPushKeyRes]): Unit = {
      requests.add(request)
      val swapHash = ByteVector32(ByteVector(request.getSwapHash.toByteArray))
      pushedKeys.put(swapHash, PrivateKey(ByteVector32(ByteVector(request.getInternalPrivkey.toByteArray))))
      reply(observer, ServerPushKeyRes.newBuilder().build())
    }

    override def fetchL402(request: FetchL402Request, observer: StreamObserver[FetchL402Response]): Unit = {
      requests.add(request)
      reply(observer, FetchL402Response.newBuilder().build())
    }
  }

  val serviceDefinition: ServerServiceDefinition = ServerInterceptors.intercept(service, aperture)

}

/** Pays fake swap server invoices by looking up their preimage. */
class FakeL402Payer(server: FakeSwapServer) extends L402Payer {

  val payments = new AtomicInteger(0)
  @volatile var result: Bolt11Invoice => Future[PaymentResult] = invoice => Future.successful(Paid(server.preimages(invoice.paymentHash), invoice.amount_opt.get, MilliSatoshi(1000)))
  @volatile var statuses: Map[ByteVector32, PaymentStatus] = Map.empty
  @volatile var lastMaxFee: Option[Satoshi] = None

  override def pay(invoice: Bolt11Invoice, maxFee: Satoshi, timeout: FiniteDuration): Future[PaymentResult] = {
    payments.incrementAndGet()
    lastMaxFee = Some(maxFee)
    result(invoice)
  }

  override def status(paymentHash: ByteVector32): PaymentStatus = statuses.getOrElse(paymentHash, PaymentStatus.Unknown)

  /** A payment that never completes until we complete the promise. */
  def blockPayments(): Promise[PaymentResult] = {
    val p = Promise[PaymentResult]()
    result = _ => p.future
    p
  }

}
