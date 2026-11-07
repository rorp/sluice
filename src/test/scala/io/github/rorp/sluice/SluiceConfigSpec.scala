package io.github.rorp.sluice

import com.typesafe.config.ConfigFactory
import fr.acinq.bitcoin.scalacompat.{Block, SatoshiLong}
import org.scalatest.funsuite.AnyFunSuite

class SluiceConfigSpec extends AnyFunSuite {

  test("read our section of eclair's configuration") {
    val eclairConfig = ConfigFactory.parseString(
      """
        |sluice {
        |  server = "localhost:11009"
        |  tls = "insecure"
        |  max-swap-fee-ppm = 1000
        |}
        |""".stripMargin)
    val config = SluiceConfig(eclairConfig, Block.RegtestGenesisBlock.hash, "0.14.6")
    assert(config.serverAddress == "localhost:11009")
    assert(config.tls == ServerTls.Insecure)
    assert(config.maxSwapFeePpm == 1000)
    // Defaults.
    assert(config.userAgent == "sluice/v0.1.0-alpha/eclair=0.14.6")
    assert(config.maxPrepay == 30_000.sat)
  }

  test("use Lightning Labs' server by default on testnet") {
    val config = SluiceConfig(ConfigFactory.empty(), Block.Testnet3GenesisBlock.hash, "0.14.6")
    assert(config.serverAddress == "test.swap.lightning.today:11010")
    assert(config.tls == ServerTls.SystemCa)
  }

  test("refuse the section of earlier versions, whose settings we would ignore") {
    val eclairConfig = ConfigFactory.parseString("""loop.server = "localhost:11009"""")
    val e = intercept[IllegalArgumentException](SluiceConfig(eclairConfig, Block.RegtestGenesisBlock.hash, "0.14.6"))
    assert(e.getMessage.contains("moved from eclair.loop to eclair.sluice"))
  }

}
