package io.github.rorp.sluice

import fr.acinq.bitcoin.SigHash.SIGHASH_DEFAULT
import fr.acinq.bitcoin.scalacompat.Crypto.{PrivateKey, PublicKey, XonlyPublicKey}
import fr.acinq.bitcoin.scalacompat._
import fr.acinq.eclair.BlockHeight
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import fr.acinq.eclair.transactions.Transactions
import scodec.bits.ByteVector

/**
 * Taproot swap htlc (loop's HtlcScriptV3, swap/htlc.go), used by both loop out and loop in.
 *
 * The output key is the MuSig2 aggregate of the sender and receiver internal keys (sorted), tweaked with a script tree:
 *  - success leaf: `<receiver> OP_CHECKSIGVERIFY OP_SIZE 32 OP_EQUALVERIFY OP_HASH160 <ripemd160(swap_hash)> OP_EQUALVERIFY 1 OP_CHECKSEQUENCEVERIFY`
 *  - timeout leaf: `<sender> OP_CHECKSIGVERIFY <cltv_expiry> OP_CHECKLOCKTIMEVERIFY`
 *
 * For loop out, the server is the sender and we are the receiver, and the same key is used as internal and script key.
 */
case class Htlc(swapHash: ByteVector32,
                cltvExpiry: BlockHeight,
                senderInternalKey: PublicKey,
                receiverInternalKey: PublicKey,
                senderKey: PublicKey,
                receiverKey: PublicKey) {

  import Htlc._

  val successScript: Seq[ScriptElt] = OP_PUSHDATA(receiverKey.xOnly) :: OP_CHECKSIGVERIFY ::
    OP_SIZE :: encodeNumber(32) :: OP_EQUALVERIFY ::
    OP_HASH160 :: OP_PUSHDATA(Crypto.ripemd160(swapHash)) :: OP_EQUALVERIFY ::
    encodeNumber(1) :: OP_CHECKSEQUENCEVERIFY :: Nil

  val timeoutScript: Seq[ScriptElt] = OP_PUSHDATA(senderKey.xOnly) :: OP_CHECKSIGVERIFY :: encodeNumber(cltvExpiry.toLong) :: OP_CHECKLOCKTIMEVERIFY :: Nil

  val successLeaf: ScriptTree.Leaf = ScriptTree.Leaf(successScript)
  val timeoutLeaf: ScriptTree.Leaf = ScriptTree.Leaf(timeoutScript)
  val scriptTree: ScriptTree.Branch = ScriptTree.Branch(successLeaf, timeoutLeaf)

  /** Keys sorted like MuSig2CombineKeys(sort=true) does, which is the order used for signing as well. */
  val internalKeys: Seq[PublicKey] = sortKeys(Seq(senderInternalKey, receiverInternalKey))
  val internalKey: XonlyPublicKey = Musig2.aggregateKeys(internalKeys)

  val pubkeyScript: Seq[ScriptElt] = Script.pay2tr(internalKey, scriptTree)
  val pubkeyScriptBytes: ByteVector = Script.write(pubkeyScript)

  def address(chainHash: BlockHash): String = addressFromPublicKeyScript(chainHash, pubkeyScript).getOrElse(throw new IllegalStateException("invalid htlc script"))

  def successWitness(preimage: ByteVector32, receiverSig: ByteVector64): ScriptWitness =
    Script.witnessScriptPathPay2tr(internalKey, successLeaf, ScriptWitness(Seq(preimage, receiverSig)), scriptTree)

  def timeoutWitness(senderSig: ByteVector64): ScriptWitness =
    Script.witnessScriptPathPay2tr(internalKey, timeoutLeaf, ScriptWitness(Seq(senderSig)), scriptTree)

  /** Returns the preimage if this witness spends the htlc through the success leaf. */
  def extractPreimage(witness: ScriptWitness): Option[ByteVector32] = witness.stack match {
    case preimage :: _ :: script :: _ :: Nil if preimage.size == 32 && script == Script.write(successScript) && Crypto.sha256(preimage) == swapHash => Some(ByteVector32(preimage))
    case _ => None
  }

  /** preimage, signature, success script and control block (internal key + sibling leaf hash). */
  val successWitnessWeight: Int = 1 + (1 + 32) + (1 + 64) + (1 + Script.write(successScript).size.toInt) + (1 + 33 + 32)

  def sweepWeight(outputScript: ByteVector, keyPath: Boolean): Int = sweepBaseWeight(outputScript) + (if (keyPath) KeyPathWitnessWeight else successWitnessWeight)

  /** signature, timeout script and control block. */
  val timeoutWitnessWeight: Int = 1 + (1 + 64) + (1 + Script.write(timeoutScript).size.toInt) + (1 + 33 + 32)

  def refundWeight(outputScript: ByteVector): Int = sweepBaseWeight(outputScript) + timeoutWitnessWeight

  def isTimeoutWitness(witness: ScriptWitness): Boolean = witness.stack match {
    case _ :: script :: _ :: Nil => script == Script.write(timeoutScript)
    case _ => false
  }

}

object Htlc {

  private def encodeNumber(n: Long): ScriptElt = n match {
    case 0 => OP_0
    case x if x >= 1 && x <= 16 => ScriptElt.code2elt((ScriptElt.elt2code(OP_1) + x - 1).toInt).get
    case _ => OP_PUSHDATA(Script.encodeNumber(n))
  }

  /** Lexicographic ordering of compressed keys (BIP 327 KeySort). */
  def sortKeys(keys: Seq[PublicKey]): Seq[PublicKey] = keys.sortWith { (a, b) => LexicographicalOrdering.isLessThan(a.value, b.value) }

  /** Sequence of the success path (1 block relative timelock). */
  val SuccessSequence: Long = 1

  /** key path witness: one 64 bytes schnorr signature (SIGHASH_DEFAULT). */
  val KeyPathWitnessWeight: Int = 1 + 1 + 64

  /** Weight of a 1-input 1-output sweep without witness, for the given output script. */
  def sweepBaseWeight(outputScript: ByteVector): Int = {
    val tx = Transaction(2, Seq(TxIn(OutPoint(TxId(ByteVector32.Zeroes), 0), Nil, 0)), Seq(TxOut(0.sat, outputScript)), 0)
    // weight of the non-witness data, plus the segwit marker and flag.
    tx.weight() + 2
  }

  /** Unsigned sweep of the htlc output paying the htlc value minus fee to the given script. */
  def sweepTx(htlcOutPoint: OutPoint, htlcAmount: Satoshi, outputScript: ByteVector, fee: Satoshi, keyPath: Boolean): Transaction = Transaction(
    version = 2,
    txIn = Seq(TxIn(htlcOutPoint, ByteVector.empty, if (keyPath) TxIn.SEQUENCE_FINAL - 2 else SuccessSequence)),
    txOut = Seq(TxOut(htlcAmount - fee, outputScript)),
    lockTime = 0
  )

  /** Unsigned refund of the htlc through the timeout path (valid from the htlc expiry). */
  def refundTx(htlc: Htlc, htlcOutPoint: OutPoint, htlcAmount: Satoshi, outputScript: ByteVector, fee: Satoshi): Transaction = Transaction(
    version = 2,
    txIn = Seq(TxIn(htlcOutPoint, ByteVector.empty, TxIn.SEQUENCE_FINAL - 2)),
    txOut = Seq(TxOut(htlcAmount - fee, outputScript)),
    lockTime = htlc.cltvExpiry.toLong
  )

  /** Signs the timeout path of the htlc (loop in refund). */
  def signTimeout(htlc: Htlc, tx: Transaction, htlcOutput: TxOut, senderKey: PrivateKey): Transaction = {
    require(senderKey.publicKey == htlc.senderKey, "invalid sender key")
    val sig = tx.signInputTaprootScriptPath(senderKey, 0, Seq(htlcOutput), SIGHASH_DEFAULT, htlc.timeoutLeaf.hash())
    tx.updateWitness(0, htlc.timeoutWitness(sig))
  }

  /**
   * Fee of a transaction replacing previousFee_opt (if any) at the given feerate, or None if we should keep the
   * previous one: a replacement must pay more than the previous fee plus its own relay fee (BIP 125), and we never pay
   * more than maxFee.
   */
  def replacementFee(feerate: FeeratePerKw, weight: Int, maxFee: Satoshi, previousFee_opt: Option[Satoshi]): Option[Satoshi] = {
    val targetFee = Transactions.weight2fee(feerate, weight)
    val fee_opt = previousFee_opt match {
      case None => Some(targetFee)
      case Some(previousFee) if targetFee <= previousFee => None
      case Some(previousFee) => Some(targetFee.max(previousFee + Satoshi((weight + 3) / 4)))
    }
    fee_opt.map(_.min(maxFee)).filter(fee => fee > Satoshi(0) && previousFee_opt.forall(fee > _))
  }

  /** Signs the success path of the htlc (loop out sweep without the server's cooperation). */
  def signSuccess(htlc: Htlc, tx: Transaction, htlcOutput: TxOut, receiverKey: PrivateKey, preimage: ByteVector32): Transaction = {
    require(receiverKey.publicKey == htlc.receiverKey, "invalid receiver key")
    val sig = tx.signInputTaprootScriptPath(receiverKey, 0, Seq(htlcOutput), SIGHASH_DEFAULT, htlc.successLeaf.hash())
    tx.updateWitness(0, htlc.successWitness(preimage, sig))
  }

}
