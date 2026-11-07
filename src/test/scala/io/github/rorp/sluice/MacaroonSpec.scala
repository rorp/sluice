package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.ByteVector32
import org.scalatest.funsuite.AnyFunSuite
import scodec.bits.{ByteVector, HexStringSyntax}

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class MacaroonSpec extends AnyFunSuite {

  private def hmac(key: ByteVector, data: ByteVector): ByteVector = {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key.toArray, "HmacSHA256"))
    ByteVector(mac.doFinal(data.toArray))
  }

  // Signature of a new macaroon, as computed by go-macaroon's New(): the root key is first derived with a fixed key.
  private def rootSignature(rootKey: ByteVector, id: ByteVector): ByteVector32 =
    ByteVector32(hmac(hmac(ByteVector("macaroons-key-generator".getBytes("US-ASCII")), rootKey), id))

  private val rootKey = hex"0101010101010101010101010101010101010101010101010101010101010101"

  test("encode and decode v2 binary macaroons") {
    val sig = rootSignature(rootKey, hex"6964")
    val m = Macaroon(Some(ByteVector("loc".getBytes)), hex"6964", Nil, sig)
    val expected = hex"02" ++ hex"01036c6f63" ++ hex"02026964" ++ hex"00" ++ hex"00" ++ hex"0620" ++ sig.bytes
    assert(m.encode() == expected)
    assert(Macaroon.decode(expected).get == m)
    // location is optional
    val noLocation = hex"02" ++ hex"02026964" ++ hex"00" ++ hex"00" ++ hex"0620" ++ sig.bytes
    assert(Macaroon.decode(noLocation).get == m.copy(location_opt = None))
    assert(m.copy(location_opt = None).encode() == noLocation)
  }

  test("add first-party caveats") {
    val sig0 = rootSignature(rootKey, hex"6964")
    val m = Macaroon(None, hex"6964", Nil, sig0).addFirstPartyCaveat(ByteVector("a=b".getBytes)).addFirstPartyCaveat(ByteVector("c=d".getBytes))
    val sig1 = hmac(sig0.bytes, ByteVector("a=b".getBytes))
    val sig2 = hmac(sig1, ByteVector("c=d".getBytes))
    assert(m.signature.bytes == sig2)
    val expected = hex"02" ++ hex"02026964" ++ hex"00" ++ hex"0203613d62" ++ hex"00" ++ hex"0203633d64" ++ hex"00" ++ hex"00" ++ hex"0620" ++ sig2
    assert(m.encode() == expected)
    assert(Macaroon.decode(expected).get == m)
  }

  test("encode fields longer than 127 bytes") {
    val longCaveat = ByteVector.fill(200)(0x61)
    val m = Macaroon(None, hex"6964", Nil, rootSignature(rootKey, hex"6964")).addFirstPartyCaveat(longCaveat)
    val encoded = m.encode()
    assert(encoded.containsSlice(hex"02c801" ++ longCaveat))
    assert(Macaroon.decode(encoded).get == m)
  }

  test("decode third-party caveats") {
    val sig = rootSignature(rootKey, hex"6964")
    val bin = hex"02" ++ hex"02026964" ++ hex"00" ++ hex"01027463" ++ hex"02026363" ++ hex"04027676" ++ hex"00" ++ hex"00" ++ hex"0620" ++ sig.bytes
    val m = Macaroon.decode(bin).get
    assert(m.caveats == Seq(Macaroon.Caveat(Some(hex"7463"), hex"6363", Some(hex"7676"))))
    assert(m.encode() == bin)
  }

  test("reject invalid macaroons") {
    val sig = rootSignature(rootKey, hex"6964").bytes
    val invalid = Seq(
      ByteVector.empty,
      hex"01" ++ hex"02026964" ++ hex"00" ++ hex"00" ++ hex"0620" ++ sig, // v1 is not supported
      hex"02" ++ hex"00" ++ hex"00" ++ hex"0620" ++ sig, // missing identifier
      hex"02" ++ hex"02026964" ++ hex"01036c6f63" ++ hex"00" ++ hex"00" ++ hex"0620" ++ sig, // fields out of order
      hex"02" ++ hex"02026964" ++ hex"00" ++ hex"00" ++ hex"061f" ++ sig.take(31), // invalid signature length
      hex"02" ++ hex"02026964" ++ hex"00" ++ hex"00" ++ hex"0620" ++ sig.take(10), // truncated
      hex"02" ++ hex"02026964" ++ hex"00" ++ hex"00" ++ hex"0620" ++ sig ++ hex"00", // trailing data
      hex"02" ++ hex"02026964" ++ hex"00" ++ hex"01027463" ++ hex"02026363" ++ hex"00" ++ hex"00" ++ hex"0620" ++ sig, // first-party caveat with location
    )
    invalid.foreach(bin => assert(Macaroon.decode(bin).isFailure, bin.toHex))
  }

}
