package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.Crypto
import fr.acinq.bitcoin.scalacompat.Crypto.PrivateKey
import fr.acinq.eclair.randomBytes32
import org.scalatest.funsuite.AnyFunSuite
import scodec.bits.{ByteVector, HexStringSyntax}

class LoopKeysSpec extends AnyFunSuite {

  test("our loop in internal keys are unique per swap, even if key indexes are reused") {
    val keys = new LoopKeys(randomBytes32().bytes)
    val (swapHash1, swapHash2) = (randomBytes32(), randomBytes32())
    assert(keys.internalKey(0, swapHash1) == keys.internalKey(0, swapHash1))
    assert(keys.internalKey(0, swapHash1) != keys.internalKey(0, swapHash2))
    assert(keys.internalKey(0, swapHash1) != keys.internalKey(1, swapHash1))
    assert(keys.internalKey(0, swapHash1) != keys.swapKey(0))
  }

  test("key derivation never changes (pending swaps depend on it)") {
    // BIP32 test vector 1 seed, keys computed independently: m/0'/i' and HMAC-SHA256(m/2'/i', swap hash).
    val keys = new LoopKeys(hex"000102030405060708090a0b0c0d0e0f")
    val swapHash = Crypto.sha256(ByteVector("eclair-loop".getBytes("US-ASCII")))
    assert(keys.swapKey(0).value.bytes == hex"c6b635b751716f6bfd99a1f2f30ccd9496b2c6414890ae6960371b263f8de68b")
    assert(keys.swapKey(7).value.bytes == hex"cf0a52a2fabdf819256165c96afc874d42f6052f09859a5872cd703952a0e4e3")
    assert(keys.internalKey(0, swapHash).value.bytes == hex"a47dd1c1787186f3147883313371b7c095ff3bd0c18a846e2c0e64699a7df339")
    assert(keys.internalKey(7, swapHash).value.bytes == hex"1363b611ead6e497ae5ebab58a925aa8de3ace715a63847f1f22304cac4a65ef")
  }

  test("our seed derivation from the node key never changes (pending swaps depend on it)") {
    // Computed independently: seed = HMAC-SHA256(node key, "sluice-seed"), then the derivations above.
    val keys = LoopKeys.fromNodeKey(PrivateKey(hex"0101010101010101010101010101010101010101010101010101010101010101"))
    val swapHash = Crypto.sha256(ByteVector("sluice".getBytes("US-ASCII")))
    assert(keys.swapKey(0).value.bytes == hex"739e75ae9ac71bb7826c0d554836cea8af0b65bb3a748406082a9de5b9c3b234")
    assert(keys.internalKey(0, swapHash).value.bytes == hex"53b2339e005e147b84a5823fc544d2e407727b570bbbd676970b04bea063c0b8")
  }

}
