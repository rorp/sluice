package io.github.rorp.sluice.db

import fr.acinq.bitcoin.scalacompat.{ByteVector32, Satoshi}
import fr.acinq.eclair.{BlockHeight, MilliSatoshi, TimestampMilli}
import scodec.bits.ByteVector

sealed trait SwapType {
  def name: String
}

object SwapType {
  case object LoopOut extends SwapType {
    override val name = "loop_out"
  }

  case object LoopIn extends SwapType {
    override val name = "loop_in"
  }

  def fromName(name: String): SwapType = name match {
    case LoopOut.name => LoopOut
    case LoopIn.name => LoopIn
    case _ => throw new IllegalArgumentException(s"unknown swap type $name")
  }
}

/**
 * The API request that created a swap, when its caller gave an idempotency key: a retry of that request returns the
 * swap instead of creating another one.
 *
 * @param requestHash hash of the request's parameters: the key can't be used for another request.
 */
case class SwapRequestId(idempotencyKey: String, requestHash: ByteVector32)

/**
 * A swap and its current state.
 *
 * @param keyIndex      index of our swap key in the plugin's key derivation path.
 * @param cltvExpiry    absolute expiry of the on-chain htlc.
 * @param contract      serialized swap-type-specific contract (invoices, server keys, limits, htlc outpoint...), whose
 *                      format is owned by the swap state machines.
 * @param requestId_opt the request that created the swap, if it had an idempotency key.
 */
case class SwapRecord(swapHash: ByteVector32,
                      swapType: SwapType,
                      state: String,
                      amount: Satoshi,
                      preimage: ByteVector32,
                      keyIndex: Long,
                      cltvExpiry: BlockHeight,
                      contract: ByteVector,
                      createdAt: TimestampMilli,
                      updatedAt: TimestampMilli,
                      requestId_opt: Option[SwapRequestId] = None)

case class SwapUpdate(swapHash: ByteVector32, state: String, createdAt: TimestampMilli)

/**
 * An L402 token used to authenticate to the swap server. The token is pending until we have paid its invoice and
 * obtained the preimage.
 */
case class L402Token(paymentHash: ByteVector32,
                     baseMacaroon: ByteVector,
                     preimage_opt: Option[ByteVector32],
                     amountPaid: MilliSatoshi,
                     routingFeePaid: MilliSatoshi,
                     createdAt: TimestampMilli) {
  val isPending: Boolean = preimage_opt.isEmpty
}

trait LoopDb {

  /**
   * Adds a new swap and records its initial state. Fails if a swap with the same hash, key index or idempotency key
   * already exists.
   */
  def addSwap(swap: SwapRecord): Unit

  /** Updates the state and contract of an existing swap, and records the state transition. */
  def updateSwap(swapHash: ByteVector32, state: String, contract: ByteVector, updatedAt: TimestampMilli): Unit

  def getSwap(swapHash: ByteVector32): Option[SwapRecord]

  def listSwaps(): Seq[SwapRecord]

  /** The swap created by the request with this idempotency key, if any. */
  def getSwapByIdempotencyKey(idempotencyKey: String): Option[SwapRecord]

  def listSwapUpdates(swapHash: ByteVector32): Seq[SwapUpdate]

  /** Returns the next unused swap key index. */
  def nextKeyIndex(): Long

  /** Inserts or replaces an L402 token. */
  def storeL402Token(token: L402Token): Unit

  /** Returns the most recently created L402 token, which may be pending. */
  def currentL402Token(): Option[L402Token]

  def listL402Tokens(): Seq[L402Token]

  /** Removes a pending token (its payment failed): paid tokens are never removed. */
  def removePendingL402Token(paymentHash: ByteVector32): Unit

  def close(): Unit

}
