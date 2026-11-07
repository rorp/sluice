package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.ByteVector32
import fr.acinq.bitcoin.scalacompat.Crypto.PrivateKey
import fr.acinq.bitcoin.scalacompat.DeterministicWallet
import fr.acinq.bitcoin.scalacompat.DeterministicWallet.{ExtendedPrivateKey, hardened}
import scodec.bits.ByteVector

/**
 * Swap keys are derived from a seed specific to this plugin, itself derived from the node's private key: they are
 * recovered with eclair's seed. Each swap gets a new key index, but indexes start over from 0 if the plugin's database
 * is lost: our htlc keys may then be reused, which is fine since we only sign with them, but we give our loop in
 * internal keys to the server, so they also depend on the swap hash.
 */
class LoopKeys(seed: ByteVector) {

  private val master: ExtendedPrivateKey = DeterministicWallet.generate(seed)

  /** Our htlc key for the swap with the given key index. */
  def swapKey(keyIndex: Long): PrivateKey = derive(0, keyIndex)

  /**
   * Our MuSig2 internal key for a loop in. It is distinct from our htlc key because we give it to the server once our
   * invoice has been paid (so that it can sweep the htlc through the key path): no other swap may use it, even if
   * its key index is reused. We derive it from the swap hash, that we chose at random, with a private key that we
   * never reveal: the server knows swap hashes, so it could remove a tweak computed from the swap hash alone from the
   * keys we gave it.
   */
  def internalKey(keyIndex: Long, swapHash: ByteVector32): PrivateKey = PrivateKey(Macaroon.hmac256(derive(2, keyIndex).value, swapHash))

  // Key family 1 is unused: eclair-loop derived loop in internal keys from it, from another seed.
  private def derive(family: Long, keyIndex: Long): PrivateKey = {
    require(keyIndex >= 0 && keyIndex < hardened(0), "invalid key index")
    DeterministicWallet.derivePrivateKey(master, Seq(hardened(family), hardened(keyIndex))).privateKey
  }

}

object LoopKeys {

  // Changing the seed's label would change all our keys: we couldn't sweep or refund pending swaps anymore.
  def fromNodeKey(nodeKey: PrivateKey): LoopKeys = new LoopKeys(Macaroon.hmac256(nodeKey.value, ByteVector("sluice-seed".getBytes("US-ASCII"))).bytes)

}
