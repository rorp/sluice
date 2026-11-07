package io.github.rorp.sluice

import fr.acinq.bitcoin.ScriptFlags
import fr.acinq.bitcoin.SigHash.SIGHASH_DEFAULT
import fr.acinq.bitcoin.scalacompat.Crypto.PrivateKey
import fr.acinq.bitcoin.scalacompat.{Block, ByteVector32, Crypto, Musig2, OutPoint, SatoshiLong, Script, Transaction, TxId, TxOut}
import fr.acinq.eclair.{BlockHeight, randomBytes32}
import org.scalatest.funsuite.AnyFunSuite
import scodec.bits.{ByteVector, HexStringSyntax}

class HtlcSpec extends AnyFunSuite {

  private def key(b: Byte): PrivateKey = PrivateKey(ByteVector.fill(32)(b))

  private val preimage = ByteVector32(ByteVector.fill(32)(0x42))
  private val swapHash = Crypto.sha256(preimage)
  private val (sender, receiver, senderInternal, receiverInternal) = (key(1), key(2), key(3), key(4))

  // Generated with loop's swap.NewHtlcV3 (fd3ed6c, MuSig2Version100RC2) on regtest.
  test("loop out htlc matches loop's htlc v3") {
    assert(swapHash == ByteVector32.fromValidHex("425ed4e4a36b30ea21b90e21c712c649e8214c29b7eaf68089d1039c6e55384c"))
    val htlc = Htlc(swapHash, BlockHeight(800_123), sender.publicKey, receiver.publicKey, sender.publicKey, receiver.publicKey)
    assert(Script.write(htlc.successScript) == hex"204d4b6cd1361032ca9bd2aeb9d900aa4d45d9ead80ac9423374c451a7254d0766ad82012088a9148739f40ec4dbf569dcb38134c6e73109085669818851b2")
    assert(Script.write(htlc.timeoutScript) == hex"201b84c5567b126440995d3ed5aaba0565d71e1834604819ff9c17f5e9d5dd078fad037b350cb1")
    assert(htlc.internalKey.publicKey.value.drop(1) == hex"99a9da2f1aa3e4403438bfea7c2bc1ea9956469297f4f931c2531c24c38d5994")
    assert(htlc.pubkeyScriptBytes == hex"512032ad44ec4466f70c0ec44e69ab4f84e1a86635a0ec24dd88d7be34b4b39bae62")
    assert(htlc.address(Block.RegtestGenesisBlock.hash) == "bcrt1px2k5fmzyvmmscrkyfe56knuyux5xvddqasjdmzxhhc6tfvum4e3qjdy2f2")
    val witness = htlc.successWitness(preimage, ByteVector64Zeroes)
    assert(witness.stack.last == hex"c099a9da2f1aa3e4403438bfea7c2bc1ea9956469297f4f931c2531c24c38d599426930dbcec0d5c25a8cd150f4c5147f8ed7f424eed3cbbad550dd32ef2eaf22e")
  }

  test("loop in htlc matches loop's htlc v3") {
    val htlc = Htlc(swapHash, BlockHeight(17), senderInternal.publicKey, receiverInternal.publicKey, sender.publicKey, receiver.publicKey)
    assert(htlc.pubkeyScriptBytes == hex"5120db60520ef86a942309ba8fea4efb830addfe1e972c6b3836db8673349d494ba7")
    assert(htlc.address(Block.RegtestGenesisBlock.hash) == "bcrt1pmds9yrhcd22zxzd63l4ya7urptwlu85h934nsdkmseenf82ffwnsqr8xlk")
  }

  private val ByteVector64Zeroes = fr.acinq.bitcoin.scalacompat.ByteVector64.Zeroes

  private def fundedHtlc(htlc: Htlc): (OutPoint, TxOut) = (OutPoint(TxId(randomBytes32()), 1), TxOut(500_000 sat, htlc.pubkeyScript))

  test("sweep htlc with the preimage (script path)") {
    val htlc = Htlc(swapHash, BlockHeight(800_123), sender.publicKey, receiver.publicKey, sender.publicKey, receiver.publicKey)
    val (outPoint, txOut) = fundedHtlc(htlc)
    val outputScript = Script.write(Script.pay2wpkh(randomKeyPub()))
    val fee = 1000 sat
    val tx = Htlc.signSuccess(htlc, Htlc.sweepTx(outPoint, txOut.amount, outputScript, fee, keyPath = false), txOut, receiver, preimage)
    Transaction.correctlySpends(tx, Map(outPoint -> txOut), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
    assert(tx.txIn.head.sequence == 1)
    assert(tx.txOut.head.amount == txOut.amount - fee)
    assert(htlc.extractPreimage(tx.txIn.head.witness).contains(preimage))
    assert(!htlc.isTimeoutWitness(tx.txIn.head.witness))
    // Our weight estimate is exact (schnorr signatures have a fixed size).
    assert(tx.weight() == htlc.sweepWeight(outputScript, keyPath = false))
    // A wrong preimage can't spend the htlc.
    val invalid = Htlc.signSuccess(htlc, Htlc.sweepTx(outPoint, txOut.amount, outputScript, fee, keyPath = false), txOut, receiver, randomBytes32())
    assertThrows[Exception](Transaction.correctlySpends(invalid, Map(outPoint -> txOut), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS))
  }

  test("sweep htlc after expiry (timeout path)") {
    val htlc = Htlc(swapHash, BlockHeight(800_123), sender.publicKey, receiver.publicKey, sender.publicKey, receiver.publicKey)
    val (outPoint, txOut) = fundedHtlc(htlc)
    val unsigned = Htlc.sweepTx(outPoint, txOut.amount, Script.write(Script.pay2wpkh(randomKeyPub())), 1000 sat, keyPath = true).copy(lockTime = 800_123)
    val sig = unsigned.signInputTaprootScriptPath(sender, 0, Seq(txOut), SIGHASH_DEFAULT, htlc.timeoutLeaf.hash())
    val tx = unsigned.updateWitness(0, htlc.timeoutWitness(sig))
    Transaction.correctlySpends(tx, Map(outPoint -> txOut), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
    assert(htlc.isTimeoutWitness(tx.txIn.head.witness))
    assert(htlc.extractPreimage(tx.txIn.head.witness).isEmpty)
  }

  test("sweep htlc cooperatively (MuSig2 key path)") {
    val htlc = Htlc(swapHash, BlockHeight(800_123), senderInternal.publicKey, receiverInternal.publicKey, sender.publicKey, receiver.publicKey)
    val (outPoint, txOut) = fundedHtlc(htlc)
    val outputScript = Script.write(Script.pay2tr(randomKeyPub().xOnly, Crypto.TaprootTweak.KeyPathTweak))
    val tx = Htlc.sweepTx(outPoint, txOut.amount, outputScript, 1000 sat, keyPath = true)
    val nonce1 = Musig2.generateNonce(randomBytes32(), Left(senderInternal), htlc.internalKeys, None, None)
    val nonce2 = Musig2.generateNonce(randomBytes32(), Left(receiverInternal), htlc.internalKeys, None, None)
    val nonces = Seq(nonce1.publicNonce, nonce2.publicNonce)
    val sig1 = Musig2.signTaprootInput(senderInternal, tx, 0, Seq(txOut), htlc.internalKeys, nonce1.secretNonce, nonces, Some(htlc.scriptTree)).toOption.get
    val sig2 = Musig2.signTaprootInput(receiverInternal, tx, 0, Seq(txOut), htlc.internalKeys, nonce2.secretNonce, nonces, Some(htlc.scriptTree)).toOption.get
    assert(Musig2.verifyTaprootSignature(sig1, nonce1.publicNonce, senderInternal.publicKey, tx, 0, Seq(txOut), htlc.internalKeys, nonces, Some(htlc.scriptTree)))
    val sig = Musig2.aggregateTaprootSignatures(Seq(sig1, sig2), tx, 0, Seq(txOut), htlc.internalKeys, nonces, Some(htlc.scriptTree)).toOption.get
    val signed = tx.updateWitness(0, Script.witnessKeyPathPay2tr(sig))
    Transaction.correctlySpends(signed, Map(outPoint -> txOut), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
    assert(signed.weight() == htlc.sweepWeight(outputScript, keyPath = true))
  }

  test("replacement fee") {
    import fr.acinq.eclair.blockchain.fee.FeeratePerKw
    val feerate = FeeratePerKw(2500 sat)
    // First transaction: the target fee, capped.
    assert(Htlc.replacementFee(feerate, 800, 10_000 sat, None).contains(2000 sat))
    assert(Htlc.replacementFee(feerate, 800, 1500 sat, None).contains(1500 sat))
    // Replacements must pay more than the previous fee plus their own relay fee (1 sat/vbyte).
    assert(Htlc.replacementFee(feerate, 800, 10_000 sat, Some(2000 sat)).isEmpty)
    assert(Htlc.replacementFee(FeeratePerKw(2600 sat), 800, 10_000 sat, Some(2000 sat)).contains(2200 sat))
    assert(Htlc.replacementFee(FeeratePerKw(5000 sat), 800, 10_000 sat, Some(2000 sat)).contains(4000 sat))
    // We never exceed the max fee, and don't replace a transaction already paying it.
    assert(Htlc.replacementFee(FeeratePerKw(5000 sat), 800, 3000 sat, Some(2000 sat)).contains(3000 sat))
    assert(Htlc.replacementFee(FeeratePerKw(5000 sat), 800, 3000 sat, Some(3000 sat)).isEmpty)
  }

  test("refund htlc after expiry (loop in)") {
    val htlc = Htlc(swapHash, BlockHeight(800_123), senderInternal.publicKey, receiverInternal.publicKey, sender.publicKey, receiver.publicKey)
    val (outPoint, txOut) = fundedHtlc(htlc)
    val outputScript = Script.write(Script.pay2wpkh(randomKeyPub()))
    val tx = Htlc.signTimeout(htlc, Htlc.refundTx(htlc, outPoint, txOut.amount, outputScript, 1000 sat), txOut, sender)
    Transaction.correctlySpends(tx, Map(outPoint -> txOut), ScriptFlags.STANDARD_SCRIPT_VERIFY_FLAGS)
    assert(tx.lockTime == 800_123)
    assert(tx.weight() == htlc.refundWeight(outputScript))
    assert(htlc.isTimeoutWitness(tx.txIn.head.witness))
    // Only the sender can refund.
    assertThrows[IllegalArgumentException](Htlc.signTimeout(htlc, tx, txOut, receiver))
  }

  private def randomKeyPub() = fr.acinq.eclair.randomKey().publicKey

}
