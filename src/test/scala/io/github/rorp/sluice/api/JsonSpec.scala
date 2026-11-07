package io.github.rorp.sluice.api

import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.bitcoin.scalacompat.{ByteVector32, SatoshiLong}
import fr.acinq.eclair.api.serde.JsonSupport
import io.github.rorp.sluice.SwapManager.{L402TokenInfo, LoopInQuote, LoopOutQuote}
import io.github.rorp.sluice.SwapServerClient.LoopOutTerms
import fr.acinq.eclair.{BlockHeight, MilliSatoshi, TimestampMilli, TimestampSecond}
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.funsuite.AnyFunSuite
import scodec.bits.HexStringSyntax

class JsonSpec extends AnyFunSuite {

  import JsonSupport.{formats, serialization}

  private def json(o: AnyRef): org.json4s.JValue = parse(serialization.write(o))

  test("serialize api responses") {
    assert(json(LoopOutTerms(250_000 sat, 5_000_000 sat, 20, 60)) == parse("""{"minSwapAmount":250000,"maxSwapAmount":5000000,"minCltvDelta":20,"maxCltvDelta":60}"""))
    val dest = PublicKey(hex"02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f283686619")
    val quote = LoopOutQuote(1_000_000 sat, 10_000 sat, 1337 sat, dest, 9, BlockHeight(800_020), TimestampSecond(1700000000))
    assert(json(quote) == parse(
      s"""{"amount":1000000,"swapFee":10000,"prepayAmount":1337,"swapPaymentDest":"${dest.toHex}","confTarget":9,"cltvExpiry":800020,
         |"swapPublicationDeadline":{"iso":"2023-11-14T22:13:20Z","unix":1700000000}}""".stripMargin))
    assert(json(LoopInQuote(1_000_000 sat, 5000 sat, 1000, 6, 1525 sat)) == parse("""{"amount":1000000,"swapFee":5000,"cltvDelta":1000,"htlcConfTarget":6,"htlcPublishFee":1525}"""))
    val token = L402TokenInfo(ByteVector32.One, pending = false, MilliSatoshi(100_000), MilliSatoshi(10), TimestampMilli(1700000000000L))
    assert(json(token) == parse(
      s"""{"paymentHash":"${ByteVector32.One.toHex}","pending":false,"amountPaid":100000,"routingFeePaid":10,
         |"createdAt":{"iso":"2023-11-14T22:13:20Z","unix":1700000000}}""".stripMargin))
  }

}
