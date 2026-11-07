package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.{Block, Crypto}
import fr.acinq.eclair.payment.Bolt11Invoice
import io.github.rorp.sluice.db.L402Token
import fr.acinq.eclair.{CltvExpiryDelta, MilliSatoshi, TimestampMilli, randomBytes32, randomKey}
import io.grpc.{Metadata, Status}
import org.scalatest.funsuite.AnyFunSuite
import scodec.bits.ByteVector

import java.util.Base64

class L402Spec extends AnyFunSuite {

  private val preimage = randomBytes32()
  private val paymentHash = Crypto.sha256(preimage)
  private val macaroon = Macaroon(None, ByteVector(0, 0) ++ paymentHash.bytes ++ randomBytes32().bytes, Nil, randomBytes32())
  private val invoice = Bolt11Invoice(Block.RegtestGenesisBlock.hash, Some(MilliSatoshi(1000)), paymentHash, randomKey(), Left("l402"), CltvExpiryDelta(18))

  private def trailers(headers: String*): Metadata = {
    val md = new Metadata()
    headers.foreach(md.put(L402.AuthHeader, _))
    md
  }

  private def header(m: Macaroon, i: Bolt11Invoice, scheme: String = "L402"): String =
    s"""$scheme macaroon="${Base64.getEncoder.encodeToString(m.encode().toArray)}", invoice="${i.toString}""""

  test("detect payment required errors") {
    assert(L402.isPaymentRequired(Status.INTERNAL.withDescription("payment required").asRuntimeException()))
    assert(L402.isPaymentRequired(Status.UNKNOWN.withDescription("Payment Required").asRuntimeException()))
    assert(!L402.isPaymentRequired(Status.INTERNAL.withDescription("internal error").asRuntimeException()))
    assert(!L402.isPaymentRequired(Status.UNAVAILABLE.withDescription("payment required").asRuntimeException()))
    assert(!L402.isPaymentRequired(new RuntimeException("payment required")))
  }

  test("parse challenges") {
    val regtest = Block.RegtestGenesisBlock.hash
    assert(L402.parseChallenge(trailers(header(macaroon, invoice)), regtest).get == L402.Challenge(macaroon, invoice))
    assert(L402.parseChallenge(trailers(header(macaroon, invoice, "LSAT")), regtest).get == L402.Challenge(macaroon, invoice))
    // we pick the first valid header
    assert(L402.parseChallenge(trailers("Basic realm=\"foo\"", header(macaroon, invoice)), regtest).get == L402.Challenge(macaroon, invoice))
    // invalid challenges
    assert(L402.parseChallenge(new Metadata(), regtest).failed.get.getMessage.contains("auth header not found"))
    assert(L402.parseChallenge(trailers("Basic realm=\"foo\""), regtest).failed.get.getMessage.contains("invalid auth header format"))
    assert(L402.parseChallenge(trailers("L402 macaroon=\"not base64!\", invoice=\"lnbcrt1\""), regtest).isFailure)
    assert(L402.parseChallenge(trailers(header(macaroon, invoice)), Block.LivenetGenesisBlock.hash).failed.get.getMessage.contains("another chain"))
    val otherInvoice = Bolt11Invoice(regtest, Some(MilliSatoshi(1000)), randomBytes32(), randomKey(), Left("l402"), CltvExpiryDelta(18))
    assert(L402.parseChallenge(trailers(header(macaroon, otherInvoice)), regtest).failed.get.getMessage.contains("doesn't match invoice payment hash"))
    val unknownVersion = macaroon.copy(identifier = ByteVector(0, 1) ++ macaroon.identifier.drop(2))
    assert(L402.parseChallenge(trailers(header(unknownVersion, invoice)), regtest).failed.get.getMessage.contains("unknown macaroon identifier version"))
  }

  test("build credentials from a paid token") {
    val pending = L402Token(paymentHash, macaroon.encode(), None, MilliSatoshi(0), MilliSatoshi(0), TimestampMilli.now())
    assert(L402.credentials(pending).isFailure)
    val paid = pending.copy(preimage_opt = Some(preimage))
    val md = L402.credentials(paid).get
    val sent = Macaroon.decode(ByteVector.fromValidHex(md.get(L402.MacaroonHeader))).get
    assert(sent == macaroon.addFirstPartyCaveat(ByteVector(s"preimage=${preimage.toHex}".getBytes)))
  }

}
