package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.bitcoin.scalacompat.{Satoshi, Transaction}
import fr.acinq.eclair.BlockHeight
import fr.acinq.eclair.wire.protocol.CommonCodecs._
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs._

/**
 * Fee limits of a loop in (see loopd's LoopInRequest).
 *
 * @param maxSwapFee  max server fee (swap amount - swap invoice amount).
 * @param maxMinerFee max fee of our htlc transaction, and of our refund if the server doesn't pay us.
 */
case class LoopInLimits(maxSwapFee: Satoshi, maxMinerFee: Satoshi)

/**
 * What we agreed on with the server when creating the swap: it never changes afterwards.
 *
 * @param swapInvoice         our invoice, that the server pays once our htlc is confirmed.
 * @param serverKey           the server's htlc key (receiver key).
 * @param serverInternalKey   the server's MuSig2 internal key.
 * @param htlcConfTarget      confirmation target of our htlc transaction.
 * @param refundScript        where we send the funds back if the server doesn't pay us.
 * @param initiationHeight    block height when we created the swap.
 */
case class LoopInContract(swapInvoice: String,
                          serverKey: PublicKey,
                          serverInternalKey: PublicKey,
                          lastHop_opt: Option[PublicKey],
                          htlcConfTarget: Int,
                          refundScript: ByteVector,
                          limits: LoopInLimits,
                          initiationHeight: BlockHeight)

/**
 * What we did since the swap started: it is persisted after every step so that we can resume after a restart.
 *
 * @param htlcTx_opt our htlc transaction, persisted before we publish it.
 * @param htlcTxFee  the fee of our htlc transaction.
 * @param keyPushed  we revealed our MuSig2 internal key to the server (after it paid us).
 * @param refundTxs  refund transactions we published (the most recent last); any of them may confirm. Once our refund
 *                   confirmed, the last one is the refund that confirmed (it may be one that we didn't keep).
 */
case class LoopInProgress(htlcTx_opt: Option[Transaction], htlcTxFee: Satoshi, keyPushed: Boolean, refundTxs: Seq[Transaction])

case class LoopInData(contract: LoopInContract, progress: LoopInProgress)

object LoopInData {

  private val limitsCodec: Codec[LoopInLimits] = (("maxSwapFee" | satoshi) :: ("maxMinerFee" | satoshi)).as[LoopInLimits]

  private def contractCodec(legacyInternalKey: Codec[Unit]): Codec[LoopInContract] = (
    ("swapInvoice" | variableSizeBytes(uint16, utf8)) ::
      ("serverKey" | publicKey) ::
      ("serverInternalKey" | publicKey) ::
      ("lastHop_opt" | optional(bool8, publicKey)) ::
      ("htlcConfTarget" | uint16) ::
      ("refundScript" | varsizebinarydata) ::
      ("limits" | limitsCodec) ::
      ("initiationHeight" | blockHeight <~ legacyInternalKey)).as[LoopInContract]

  private val transactionCodec: Codec[Transaction] = lengthDelimited(bytes.xmap(d => Transaction.read(d.toArray), d => Transaction.write(d)))

  private val progressCodec: Codec[LoopInProgress] = (
    ("htlcTx_opt" | optional(bool8, transactionCodec)) ::
      ("htlcTxFee" | satoshi) ::
      ("keyPushed" | bool8) ::
      ("refundTxs" | listOfN(uint16, transactionCodec).xmap[Seq[Transaction]](_.toSeq, _.toList))).as[LoopInProgress]

  private val dataCodecV2: Codec[LoopInData] = (("contract" | contractCodec(provide(()))) :: ("progress" | progressCodec)).as[LoopInData]

  // eclair-loop's swaps recorded whether their internal key came from its legacy derivation: Sluice can't run them
  // anyway (it derives its keys from another seed), we only read their data.
  private[sluice] val dataCodecV1: Codec[LoopInData] = (("contract" | contractCodec(bool8.unit(false))) :: ("progress" | progressCodec)).as[LoopInData]

  // eclair-loop's first swaps didn't record that flag yet: the same layout as v2.
  private[sluice] val dataCodecV0: Codec[LoopInData] = (("contract" | contractCodec(provide(()))) :: ("progress" | progressCodec)).as[LoopInData]

  val codec: Codec[LoopInData] = discriminated[LoopInData].by(uint8)
    .typecase(2, dataCodecV2)
    .typecase(1, dataCodecV1.decodeOnly)
    .typecase(0, dataCodecV0.decodeOnly)

  def encode(data: LoopInData): ByteVector = codec.encode(data).require.bytes

  def decode(bin: ByteVector): LoopInData = codec.decode(bin.bits).require.value

}
