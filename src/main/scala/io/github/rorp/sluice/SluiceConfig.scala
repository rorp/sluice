package io.github.rorp.sluice

import com.typesafe.config.{Config, ConfigFactory}
import fr.acinq.bitcoin.scalacompat.{Block, BlockHash, Satoshi, SatoshiLong}

import java.io.File
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationLong, FiniteDuration}

sealed trait ServerTls

object ServerTls {
  /** Use TLS and trust the system's certificate authorities (default for the Lightning Labs servers). */
  case object SystemCa extends ServerTls

  /** Use TLS and trust the given certificate (self-signed regtest servers). */
  case class TrustedCert(file: File) extends ServerTls

  /** Plaintext gRPC, only for local testing: L402 macaroons will be sent in the clear. */
  case object Insecure extends ServerTls
}

/**
 * @param serverAddress         host:port of the Loop swap server.
 * @param tls                   how we authenticate the swap server.
 * @param userAgent             user agent sent to the server, should follow the `Agent-Name/semver(/additional-info)` pattern.
 * @param rpcTimeout            deadline of individual unary gRPC calls.
 * @param maxL402Cost           we won't automatically pay for an L402 token that costs more than this.
 * @param maxL402RoutingFee     max routing fee when paying for an L402 token.
 * @param l402PaymentTimeout    how long we wait for an L402 payment to settle.
 * @param l402MinPurchaseInterval when the server rejects our L402 token, we don't automatically pay for a new one if we
 *                              bought one more recently than this.
 * @param defaultSweepConfTarget confirmation target used to compute the loop out htlc expiry when none is provided.
 * @param loopOutHtlcConfirmations confirmations of the server's loop out htlc before we reveal the preimage, when the
 *                              user doesn't set it: a reorg of the htlc after that lets the server take our payment and
 *                              its htlc.
 * @param maxSwapFeePpm         we refuse a swap whose quoted server fee is above this share of the swap amount, unless
 *                              the user set its max swap fee.
 * @param maxPrepay             we refuse a loop out whose quoted prepay (which the server can take without doing the
 *                              swap) is above this, unless the user set its max prepay amount.
 */
case class SluiceConfig(serverAddress: String,
                      tls: ServerTls,
                      userAgent: String,
                      rpcTimeout: FiniteDuration,
                      maxL402Cost: Satoshi,
                      maxL402RoutingFee: Satoshi,
                      l402PaymentTimeout: FiniteDuration,
                      l402MinPurchaseInterval: FiniteDuration,
                      defaultSweepConfTarget: Int,
                      loopOutHtlcConfirmations: Int,
                      maxSwapFeePpm: Long,
                      maxPrepay: Satoshi) {

  def maxSwapFee(amount: Satoshi): Satoshi = amount * maxSwapFeePpm / 1_000_000

}

object SluiceConfig {

  val PluginVersion = "0.1.0-alpha"

  // Default servers used by loopd (loopd/config.go).
  private val defaultServers: Map[BlockHash, String] = Map(
    Block.LivenetGenesisBlock.hash -> "swap.lightning.today:11010",
    Block.Testnet3GenesisBlock.hash -> "test.swap.lightning.today:11010",
  )

  // Values from loopd (l402.DefaultMaxCostSats, l402.DefaultMaxRoutingFeeSats, l402.PaymentTimeout, loopout.go
  // DefaultSweepConfTarget, loopdb.DefaultLoopOutHtlcConfirmations, and autoloop's defaultSwapFeePPM and
  // defaultMaximumPrepay for limits that the user didn't set).
  val fallback: Config = ConfigFactory.parseString(
    """
      |sluice {
      |  // server = "host:port" (defaults to the Lightning Labs server on mainnet and testnet3, required on other chains)
      |  tls = "system" // system | file | insecure
      |  tls-cert-file = ""
      |  user-agent = ""
      |  rpc-timeout = 30 seconds
      |  max-l402-cost-sat = 1000
      |  max-l402-routing-fee-sat = 10
      |  l402-payment-timeout = 60 seconds
      |  l402-min-purchase-interval = 24 hours
      |  default-sweep-conf-target = 9
      |  loop-out-htlc-confirmations = 1
      |  max-swap-fee-ppm = 5000
      |  max-prepay-sat = 30000
      |}
      |""".stripMargin)

  def defaultUserAgent(eclairVersion: String): String = s"sluice/v$PluginVersion/eclair=$eclairVersion"

  /**
   * @param config eclair's configuration (the `eclair` section), where ours is the `sluice` section.
   */
  def apply(config: Config, chainHash: BlockHash, eclairVersion: String): SluiceConfig = {
    // Our section was called loop before the plugin was renamed: we'd silently ignore its settings (its server, for
    // example), so we refuse to start until it is renamed.
    if (config.hasPath("loop")) throw new IllegalArgumentException("the plugin's configuration moved from eclair.loop to eclair.sluice: rename that section of eclair.conf")
    val c = config.withFallback(fallback).getConfig("sluice")
    val serverAddress = if (c.hasPath("server")) c.getString("server") else defaultServers.getOrElse(chainHash, throw new IllegalArgumentException("sluice.server must be configured on this chain"))
    val tls = c.getString("tls") match {
      case "system" => ServerTls.SystemCa
      case "file" =>
        val file = new File(c.getString("tls-cert-file"))
        require(file.isFile, s"sluice.tls-cert-file=${file.getAbsolutePath} does not exist")
        ServerTls.TrustedCert(file)
      case "insecure" => ServerTls.Insecure
      case other => throw new IllegalArgumentException(s"invalid sluice.tls=$other (expected system, file or insecure)")
    }
    val userAgent = Some(c.getString("user-agent")).filter(_.nonEmpty).getOrElse(defaultUserAgent(eclairVersion))
    SluiceConfig(
      serverAddress = serverAddress,
      tls = tls,
      userAgent = userAgent.take(255), // the server rejects longer user agents
      rpcTimeout = c.getDuration("rpc-timeout", TimeUnit.MILLISECONDS).millis,
      maxL402Cost = c.getLong("max-l402-cost-sat").sat,
      maxL402RoutingFee = c.getLong("max-l402-routing-fee-sat").sat,
      l402PaymentTimeout = c.getDuration("l402-payment-timeout", TimeUnit.MILLISECONDS).millis,
      l402MinPurchaseInterval = c.getDuration("l402-min-purchase-interval", TimeUnit.MILLISECONDS).millis,
      defaultSweepConfTarget = c.getInt("default-sweep-conf-target"),
      loopOutHtlcConfirmations = c.getInt("loop-out-htlc-confirmations"),
      maxSwapFeePpm = c.getLong("max-swap-fee-ppm"),
      maxPrepay = c.getLong("max-prepay-sat").sat,
    )
  }

}
