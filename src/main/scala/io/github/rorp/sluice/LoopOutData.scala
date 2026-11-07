package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.bitcoin.scalacompat.{BlockId, OutPoint, Satoshi, Transaction, TxOut}
import fr.acinq.eclair.wire.protocol.CommonCodecs._
import fr.acinq.eclair.{Alias, BlockHeight, ShortChannelId, TimestampSecond}
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs._

/**
 * Fee limits of a loop out, checked against the server's invoices and our on-chain sweep (see loopd's OutRequest).
 *
 * @param maxSwapFee          max server fee (swap invoice + prepay - swap amount).
 * @param maxPrepayAmount     max amount of the prepay invoice (we lose it if the swap fails after the server published the htlc).
 * @param maxMinerFee         max fee of our sweep transaction: we don't reveal the preimage if our sweep would cost more,
 *                            but we may pay more once the htlc is about to expire (see [[LoopOut.MaxFeeToAmountPercent]]).
 * @param maxSwapRoutingFee   max routing fee to pay the swap invoice.
 * @param maxPrepayRoutingFee max routing fee to pay the prepay invoice.
 */
case class LoopOutLimits(maxSwapFee: Satoshi, maxPrepayAmount: Satoshi, maxMinerFee: Satoshi, maxSwapRoutingFee: Satoshi, maxPrepayRoutingFee: Satoshi)

/**
 * What we agreed on with the server when creating the swap: it never changes afterwards.
 *
 * @param serverKey          the server's htlc key (sender key).
 * @param sweepScript        where we send the swap amount.
 * @param sweepConfTarget    confirmation target of our sweep, until the htlc is about to expire.
 * @param htlcConfirmations  confirmations of the server's htlc before we reveal the preimage: if the htlc is reorganized
 *                           out after that, the server can take our payment and double-spend its htlc.
 * @param initiationHeight   block height when we created the swap: the htlc can't confirm before that.
 * @param publicationDeadline the server may wait until then before publishing the htlc (to batch it with other swaps).
 * @param firstHop_opt       our channel that we pay the swap and prepay invoices through, like loopd's outgoing channel
 *                           set: the id that eclair's router used for it when we created the swap.
 */
case class LoopOutContract(swapInvoice: String,
                           prepayInvoice: String,
                           serverKey: PublicKey,
                           sweepScript: ByteVector,
                           sweepConfTarget: Int,
                           htlcConfirmations: Int,
                           limits: LoopOutLimits,
                           initiationHeight: BlockHeight,
                           publicationDeadline: TimestampSecond,
                           firstHop_opt: Option[ShortChannelId])

/** The htlc output published by the server. */
case class HtlcOutput(outPoint: OutPoint, txOut: TxOut)

/**
 * What we learnt and did since the swap started: it is persisted after every step so that we can resume after a restart.
 *
 * @param scannedHeight   all blocks up to this height have been scanned for the server's htlc.
 * @param scannedBlockId_opt the block we scanned at scannedHeight: if it isn't the parent of the next block, a reorg
 *                        replaced blocks that we scanned, and we scan them again.
 * @param htlc_opt        the server's htlc, once found in a block.
 * @param preimagePushed  the server took our preimage push (we push it until then, once we revealed it). Swaps persisted
 *                        before we retried failed pushes have it set as soon as we revealed the preimage.
 * @param sweepTxs        sweep transactions we published (the most recent last); any of them may confirm. Once the swap
 *                        succeeded, the last one is the sweep that confirmed (it may be one that we didn't keep).
 * @param musig2Failures  number of failed cooperative signing attempts: we use the script path after too many failures.
 */
case class LoopOutProgress(scannedHeight: BlockHeight,
                           scannedBlockId_opt: Option[BlockId],
                           htlc_opt: Option[HtlcOutput],
                           preimagePushed: Boolean,
                           sweepTxs: Seq[Transaction],
                           musig2Failures: Int)

case class LoopOutData(contract: LoopOutContract, progress: LoopOutProgress)

object LoopOutData {

  private val limitsCodec: Codec[LoopOutLimits] = (
    ("maxSwapFee" | satoshi) ::
      ("maxPrepayAmount" | satoshi) ::
      ("maxMinerFee" | satoshi) ::
      ("maxSwapRoutingFee" | satoshi) ::
      ("maxPrepayRoutingFee" | satoshi)).as[LoopOutLimits]

  private def contractCodec(htlcConfirmations: Codec[Int], firstHop: Codec[Option[ShortChannelId]]): Codec[LoopOutContract] = (
    ("swapInvoice" | variableSizeBytes(uint16, utf8)) ::
      ("prepayInvoice" | variableSizeBytes(uint16, utf8)) ::
      ("serverKey" | publicKey) ::
      ("sweepScript" | varsizebinarydata) ::
      ("sweepConfTarget" | uint16) ::
      ("htlcConfirmations" | htlcConfirmations) ::
      ("limits" | limitsCodec) ::
      ("initiationHeight" | blockHeight) ::
      ("publicationDeadline" | timestampSecond) ::
      ("firstHop_opt" | firstHop)).as[LoopOutContract]

  private val outPointCodec: Codec[OutPoint] = lengthDelimited(bytes.xmap(d => OutPoint.read(d.toArray), d => OutPoint.write(d)))
  private val txOutCodec: Codec[TxOut] = lengthDelimited(bytes.xmap(d => TxOut.read(d.toArray), d => TxOut.write(d)))
  private val transactionCodec: Codec[Transaction] = lengthDelimited(bytes.xmap(d => Transaction.read(d.toArray), d => Transaction.write(d)))

  private val htlcOutputCodec: Codec[HtlcOutput] = (("outPoint" | outPointCodec) :: ("txOut" | txOutCodec)).as[HtlcOutput]

  private def progressCodec(scannedBlockId: Codec[Option[BlockId]]): Codec[LoopOutProgress] = (
    ("scannedHeight" | blockHeight) ::
      ("scannedBlockId_opt" | scannedBlockId) ::
      ("htlc_opt" | optional(bool8, htlcOutputCodec)) ::
      ("preimagePushed" | bool8) ::
      ("sweepTxs" | listOfN(uint16, transactionCodec).xmap[Seq[Transaction]](_.toSeq, _.toList)) ::
      ("musig2Failures" | uint16)).as[LoopOutProgress]

  private val blockIdCodec: Codec[BlockId] = bytes32.as[BlockId]

  // We remember whether the first hop is an alias, to display it like eclair does.
  private val firstHopCodec: Codec[ShortChannelId] = discriminated[ShortChannelId].by(uint8)
    .typecase(1, alias)
    .typecase(0, shortchannelid)

  private val dataCodecV2: Codec[LoopOutData] = (
    ("contract" | contractCodec(uint16, optional(bool8, firstHopCodec))) ::
      ("progress" | progressCodec(optional(bool8, blockIdCodec)))).as[LoopOutData]

  // Swaps created before we persisted their first hop could be paid through any of our channels.
  private[sluice] val dataCodecV1: Codec[LoopOutData] = (
    ("contract" | contractCodec(uint16, provide(Option.empty[ShortChannelId]))) ::
      ("progress" | progressCodec(optional(bool8, blockIdCodec)))).as[LoopOutData]

  // Swaps created before we persisted these fields revealed the preimage after 1 confirmation, and didn't remember the
  // block they scanned last.
  private[sluice] val dataCodecV0: Codec[LoopOutData] = (
    ("contract" | contractCodec(provide(1), provide(Option.empty[ShortChannelId]))) ::
      ("progress" | progressCodec(provide(Option.empty[BlockId])))).as[LoopOutData]

  val codec: Codec[LoopOutData] = discriminated[LoopOutData].by(uint8)
    .typecase(2, dataCodecV2)
    .typecase(1, dataCodecV1.decodeOnly)
    .typecase(0, dataCodecV0.decodeOnly)

  def encode(data: LoopOutData): ByteVector = codec.encode(data).require.bytes

  def decode(bin: ByteVector): LoopOutData = codec.decode(bin.bits).require.value

}
