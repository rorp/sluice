package io.github.rorp.sluice

import fr.acinq.bitcoin.scalacompat.ByteVector32
import scodec.bits.ByteVector

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.annotation.tailrec
import scala.util.Try

/**
 * Minimal implementation of macaroons in the v2 binary format, as used by L402 (see go-macaroon's marshal-v2.go and
 * https://github.com/rescrv/libmacaroons/blob/master/doc/format.txt).
 *
 * We never verify macaroons (we don't have the root key), we only need to decode the macaroon issued by the server,
 * append first-party caveats (which updates the signature chain) and re-encode it.
 */
case class Macaroon(location_opt: Option[ByteVector], identifier: ByteVector, caveats: Seq[Macaroon.Caveat], signature: ByteVector32) {

  /** Add a first-party caveat: the new signature is HMAC-SHA256(key = previous signature, data = caveat id). */
  def addFirstPartyCaveat(caveatId: ByteVector): Macaroon =
    copy(caveats = caveats :+ Macaroon.Caveat(None, caveatId, None), signature = Macaroon.hmac256(signature.bytes, caveatId))

  def encode(): ByteVector = Macaroon.encode(this)

}

object Macaroon {

  case class Caveat(location_opt: Option[ByteVector], identifier: ByteVector, verificationId_opt: Option[ByteVector])

  private val VersionV2: Byte = 2

  private val FieldEOS = 0
  private val FieldLocation = 1
  private val FieldIdentifier = 2
  private val FieldVerificationId = 4
  private val FieldSignature = 6

  def hmac256(key: ByteVector, data: ByteVector): ByteVector32 = {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key.toArray, "HmacSHA256"))
    ByteVector32(ByteVector.view(mac.doFinal(data.toArray)))
  }

  private case class Packet(fieldType: Int, data: ByteVector)

  private def writeUVarint(value: Long): ByteVector = {
    @tailrec
    def loop(v: Long, acc: ByteVector): ByteVector =
      if ((v & ~0x7fL) == 0) acc :+ v.toByte
      else loop(v >>> 7, acc :+ ((v & 0x7f) | 0x80).toByte)

    loop(value, ByteVector.empty)
  }

  private def readUVarint(input: ByteVector): (Long, ByteVector) = {
    @tailrec
    def loop(in: ByteVector, shift: Int, acc: Long): (Long, ByteVector) = {
      require(in.nonEmpty, "unexpected end of macaroon data")
      require(shift < 64, "varint overflow")
      val b = in.head & 0xff
      val value = acc | ((b & 0x7fL) << shift)
      if ((b & 0x80) == 0) (value, in.tail) else loop(in.tail, shift + 7, value)
    }

    loop(input, 0, 0)
  }

  private def writePacket(fieldType: Int, data: ByteVector): ByteVector = writeUVarint(fieldType) ++ writeUVarint(data.size) ++ data

  private def readPacket(input: ByteVector): (Packet, ByteVector) = {
    val (fieldType, rest1) = readUVarint(input)
    if (fieldType == FieldEOS) {
      (Packet(FieldEOS, ByteVector.empty), rest1)
    } else {
      val (len, rest2) = readUVarint(rest1)
      require(len <= rest2.size, "macaroon field length exceeds remaining data")
      (Packet(fieldType.toInt, rest2.take(len)), rest2.drop(len))
    }
  }

  /** Reads packets until the end-of-section marker; fields must appear in strictly increasing order. */
  private def readSection(input: ByteVector): (Seq[Packet], ByteVector) = {
    @tailrec
    def loop(in: ByteVector, acc: Seq[Packet]): (Seq[Packet], ByteVector) = {
      val (packet, rest) = readPacket(in)
      if (packet.fieldType == FieldEOS) {
        (acc, rest)
      } else {
        require(acc.lastOption.forall(_.fieldType < packet.fieldType), "macaroon fields out of order")
        loop(rest, acc :+ packet)
      }
    }

    loop(input, Nil)
  }

  def decode(bin: ByteVector): Try[Macaroon] = Try {
    require(bin.nonEmpty && bin.head == VersionV2, "only v2 binary macaroons are supported")
    val (header, afterHeader) = readSection(bin.tail)
    val (location_opt, idFields) = header match {
      case Packet(FieldLocation, loc) +: tail => (Some(loc), tail)
      case _ => (None, header)
    }
    val identifier = idFields match {
      case Seq(Packet(FieldIdentifier, id)) => id
      case _ => throw new IllegalArgumentException("invalid macaroon header")
    }

    @tailrec
    def readCaveats(in: ByteVector, acc: Seq[Caveat]): (Seq[Caveat], ByteVector) = {
      val (section, rest) = readSection(in)
      if (section.isEmpty) {
        (acc, rest)
      } else {
        val (cavLocation_opt, cavFields) = section match {
          case Packet(FieldLocation, loc) +: tail => (Some(loc), tail)
          case _ => (None, section)
        }
        val caveat = cavFields match {
          case Seq(Packet(FieldIdentifier, id)) =>
            require(cavLocation_opt.isEmpty, "location not allowed in first party caveat")
            Caveat(None, id, None)
          case Seq(Packet(FieldIdentifier, id), Packet(FieldVerificationId, vid)) => Caveat(cavLocation_opt, id, Some(vid))
          case _ => throw new IllegalArgumentException("invalid macaroon caveat")
        }
        readCaveats(rest, acc :+ caveat)
      }
    }

    val (caveats, afterCaveats) = readCaveats(afterHeader, Nil)
    val (sig, remaining) = readPacket(afterCaveats)
    require(sig.fieldType == FieldSignature && sig.data.size == 32, "invalid macaroon signature")
    require(remaining.isEmpty, "unexpected trailing macaroon data")
    Macaroon(location_opt, identifier, caveats, ByteVector32(sig.data))
  }

  def encode(m: Macaroon): ByteVector = {
    val eos = writeUVarint(FieldEOS)
    val header = m.location_opt.map(writePacket(FieldLocation, _)).getOrElse(ByteVector.empty) ++ writePacket(FieldIdentifier, m.identifier) ++ eos
    val caveats = m.caveats.map { c =>
      c.location_opt.map(writePacket(FieldLocation, _)).getOrElse(ByteVector.empty) ++
        writePacket(FieldIdentifier, c.identifier) ++
        c.verificationId_opt.map(writePacket(FieldVerificationId, _)).getOrElse(ByteVector.empty) ++
        eos
    }.foldLeft(ByteVector.empty)(_ ++ _)
    ByteVector(VersionV2) ++ header ++ caveats ++ eos ++ writePacket(FieldSignature, m.signature.bytes)
  }

}
