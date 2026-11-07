package io.github.rorp.sluice

import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.typed.{ActorRef, Scheduler, Terminated}
import akka.util.Timeout
import fr.acinq.bitcoin.scalacompat.ByteVector32
import akka.http.scaladsl.server.Route
import fr.acinq.eclair.api.directives.EclairDirectives
import fr.acinq.eclair.blockchain.bitcoind.rpc.BitcoinCoreClient
import fr.acinq.eclair.db.Databases.PostgresDatabases
import fr.acinq.eclair.db.pg.PgUtils.PgLock
import fr.acinq.eclair.db.sqlite.SqliteUtils
import io.github.rorp.sluice.api.LoopRoutes
import io.github.rorp.sluice.db.{LoopDb, PgLoopDb, SqliteLoopDb, SwapRecord, SwapType}
import fr.acinq.eclair.{Kit, NodeParams, Plugin, PluginParams, RouteProvider, Setup}
import grizzled.slf4j.Logging

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class SluicePlugin extends Plugin with RouteProvider with Logging {

  private var setup: Setup = _
  private var config: SluiceConfig = _
  private var db: LoopDb = _
  private var server: SwapServerClient = _
  private var manager: SwapManager = _

  override def params: PluginParams = new PluginParams {
    override def name: String = "Sluice"
  }

  override def onSetup(setup: Setup): Unit = {
    this.setup = setup
    // Eclair stops before it starts running if our configuration is invalid, or if we can't open our database (eclair
    // opened its own already): errors in onKit would leave it running without us.
    config = SluiceConfig(setup.config, setup.nodeParams.chainHash, Kit.getVersionLong)
    db = SluicePlugin.openDb(setup.nodeParams, setup.chaindir)
  }

  override def onKit(kit: Kit): Unit = {
    implicit val ec: ExecutionContext = kit.system.dispatcher
    val nodeParams = kit.nodeParams
    logger.info(s"loop server=${config.serverAddress} user-agent=${config.userAgent}")
    val l402 = new L402Authenticator(db, new EclairL402Payer(kit), nodeParams.chainHash, config.maxL402Cost, config.maxL402RoutingFee, config.l402PaymentTimeout, config.l402MinPurchaseInterval)
    server = SwapServerClient(config, l402)
    val bitcoin = kit.wallet match {
      case b: BitcoinCoreClient => b
      case other => throw new IllegalArgumentException(s"sluice requires eclair's bitcoind wallet (got ${other.getClass.getName})")
    }
    val chain = new BitcoinCoreLoopChain(kit, bitcoin)
    val payments = new EclairLoopPayments(kit)
    val keys = LoopKeys.fromNodeKey(nodeParams.privateKey)
    val env = SwapEnv(server, chain, payments, db, keys, nodeParams.chainHash)
    // Running swaps, so that we can send them commands (they're removed once they stop).
    val running = new ConcurrentHashMap[ByteVector32, ActorRef[SwapActor.Command]]()
    val spawn: SwapLike => Unit = { swap =>
      val behavior = swap match {
        case s: LoopOut.Swap => LoopOut(s, env)
        case s: LoopIn.Swap => LoopIn(s, env)
      }
      val swapHash = swap.record.swapHash
      val prefix = if (swap.record.swapType == SwapType.LoopOut) "loop-out" else "loop-in"
      val actor = kit.system.spawn(behavior, s"$prefix-${swapHash.toHex}")
      running.put(swapHash, actor)
      kit.system.spawnAnonymous(Behaviors.setup[Unit] { context =>
        context.watch(actor)
        Behaviors.receiveSignal[Unit] { case (_, Terminated(_)) =>
          running.remove(swapHash, actor)
          Behaviors.stopped
        }
      })
      ()
    }
    val abandonRunning = (swapHash: ByteVector32) => Option(running.get(swapHash)) match {
      case Some(actor) =>
        implicit val timeout: Timeout = Timeout(1.minute)
        implicit val scheduler: Scheduler = kit.system.toTyped.scheduler
        actor.ask[Either[String, SwapRecord]](SwapActor.Abandon)
      case None => Future.successful(Left("swap isn't running"))
    }
    manager = new SwapManager(server, db, config, nodeParams.nodeId, chain, payments, keys, nodeParams.chainHash, spawn, abandonRunning)
    manager.resumeSwaps()
    kit.system.registerOnTermination {
      server.close()
      db.close()
    }
  }

  override def route(directives: EclairDirectives): Route = LoopRoutes(directives, manager)

}

object SluicePlugin {

  def openDb(nodeParams: NodeParams, chaindir: File): LoopDb = nodeParams.db match {
    case pg: PostgresDatabases => openDb(chaindir, Some(pg.dataSource, pg.lock))
    case _ => openDb(chaindir, None)
  }

  /**
   * When eclair uses postgres, we keep loop.sqlite only to mark it as migrated (creating it with just the mark if eclair
   * always used postgres): our swaps are in postgres, and we must not silently start again from loop.sqlite if eclair
   * switches back to sqlite.
   */
  def openDb(chaindir: File, postgres_opt: Option[(DataSource, PgLock)]): LoopDb = {
    val sqlite = SqliteUtils.openSqliteFile(chaindir, "loop.sqlite", exclusiveLock = false, journalMode = "wal", syncFlag = "full")
    try {
      postgres_opt match {
        case Some((dataSource, lock)) =>
          val db = new PgLoopDb()(dataSource, lock)
          db.migrateFromSqlite(sqlite)
          sqlite.close()
          db
        case None => new SqliteLoopDb(sqlite)
      }
    } catch {
      case t: Throwable =>
        sqlite.close()
        throw t
    }
  }

}
