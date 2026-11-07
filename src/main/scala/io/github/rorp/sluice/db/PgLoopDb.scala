package io.github.rorp.sluice.db

import fr.acinq.bitcoin.scalacompat.{ByteVector32, Satoshi}
import fr.acinq.eclair.db.pg.PgUtils
import fr.acinq.eclair.db.pg.PgUtils.PgLock
import fr.acinq.eclair.{BlockHeight, MilliSatoshi, TimestampMilli}
import grizzled.slf4j.Logging
import scodec.bits.ByteVector

import java.sql.{Connection, ResultSet, Statement, Timestamp}
import java.time.Instant
import javax.sql.DataSource

object PgLoopDb {
  val DB_NAME = "loop"
  val CURRENT_VERSION = 2
}

/** When eclair uses postgres, the loop tables live in eclair's database, in a dedicated schema. */
class PgLoopDb(implicit ds: DataSource, lock: PgLock) extends LoopDb with Logging {

  import PgLoopDb._
  import PgUtils.ExtendedResultSet._
  import PgUtils._
  import lock._

  /** v2 records the idempotency key of the request that created a swap (several swaps may have none). */
  private def migration12(statement: Statement): Unit = {
    statement.executeUpdate("ALTER TABLE loop.swaps ADD COLUMN idempotency_key TEXT, ADD COLUMN request_hash TEXT")
    statement.executeUpdate("CREATE UNIQUE INDEX swaps_idempotency_key_idx ON loop.swaps(idempotency_key)")
  }

  inTransaction { pg =>
    using(pg.createStatement()) { statement =>
      getVersion(statement, DB_NAME) match {
        case None =>
          statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS loop")
          statement.executeUpdate("CREATE TABLE loop.swaps (swap_hash TEXT NOT NULL PRIMARY KEY, swap_type TEXT NOT NULL, state TEXT NOT NULL, amount_sat BIGINT NOT NULL, preimage TEXT NOT NULL, key_index BIGINT NOT NULL UNIQUE, cltv_expiry BIGINT NOT NULL, contract BYTEA NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL, updated_at TIMESTAMP WITH TIME ZONE NOT NULL, idempotency_key TEXT, request_hash TEXT)")
          statement.executeUpdate("CREATE TABLE loop.swap_updates (id BIGSERIAL PRIMARY KEY, swap_hash TEXT NOT NULL REFERENCES loop.swaps(swap_hash), state TEXT NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL)")
          statement.executeUpdate("CREATE TABLE loop.l402_tokens (payment_hash TEXT NOT NULL PRIMARY KEY, base_macaroon BYTEA NOT NULL, preimage TEXT, amount_paid_msat BIGINT NOT NULL, routing_fee_paid_msat BIGINT NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL)")
          statement.executeUpdate("CREATE INDEX swap_updates_swap_hash_idx ON loop.swap_updates(swap_hash)")
          statement.executeUpdate("CREATE INDEX swaps_state_idx ON loop.swaps(state)")
          statement.executeUpdate("CREATE UNIQUE INDEX swaps_idempotency_key_idx ON loop.swaps(idempotency_key)")
        case Some(v@1) =>
          logger.warn(s"migrating db $DB_NAME, found version=$v current=$CURRENT_VERSION")
          migration12(statement)
        case Some(CURRENT_VERSION) => () // table is up-to-date, nothing to do
        case Some(unknownVersion) => throw new RuntimeException(s"Unknown version of DB $DB_NAME found, version=$unknownVersion")
      }
      setVersion(statement, DB_NAME, CURRENT_VERSION)
    }
  }

  private def ts(t: TimestampMilli): Timestamp = Timestamp.from(Instant.ofEpochMilli(t.toLong))

  private def insertUpdate(pg: Connection, swapHash: ByteVector32, state: String, createdAt: TimestampMilli): Unit = {
    using(pg.prepareStatement("INSERT INTO loop.swap_updates (swap_hash, state, created_at) VALUES (?, ?, ?)")) { statement =>
      statement.setString(1, swapHash.toHex)
      statement.setString(2, state)
      statement.setTimestamp(3, ts(createdAt))
      statement.executeUpdate()
    }
  }

  private def insertSwap(pg: Connection, swap: SwapRecord): Unit = {
    using(pg.prepareStatement("INSERT INTO loop.swaps (swap_hash, swap_type, state, amount_sat, preimage, key_index, cltv_expiry, contract, created_at, updated_at, idempotency_key, request_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) { statement =>
      statement.setString(1, swap.swapHash.toHex)
      statement.setString(2, swap.swapType.name)
      statement.setString(3, swap.state)
      statement.setLong(4, swap.amount.toLong)
      statement.setString(5, swap.preimage.toHex)
      statement.setLong(6, swap.keyIndex)
      statement.setLong(7, swap.cltvExpiry.toLong)
      statement.setBytes(8, swap.contract.toArray)
      statement.setTimestamp(9, ts(swap.createdAt))
      statement.setTimestamp(10, ts(swap.updatedAt))
      statement.setString(11, swap.requestId_opt.map(_.idempotencyKey).orNull)
      statement.setString(12, swap.requestId_opt.map(_.requestHash.toHex).orNull)
      statement.executeUpdate()
    }
  }

  override def addSwap(swap: SwapRecord): Unit = withLock { pg =>
    insertSwap(pg, swap)
    insertUpdate(pg, swap.swapHash, swap.state, swap.createdAt)
  }

  override def updateSwap(swapHash: ByteVector32, state: String, contract: ByteVector, updatedAt: TimestampMilli): Unit = withLock { pg =>
    val updated = using(pg.prepareStatement("UPDATE loop.swaps SET state=?, contract=?, updated_at=? WHERE swap_hash=?")) { statement =>
      statement.setString(1, state)
      statement.setBytes(2, contract.toArray)
      statement.setTimestamp(3, ts(updatedAt))
      statement.setString(4, swapHash.toHex)
      statement.executeUpdate()
    }
    if (updated != 1) throw new IllegalArgumentException(s"unknown swap $swapHash")
    insertUpdate(pg, swapHash, state, updatedAt)
  }

  private def parseSwap(rs: ResultSet): SwapRecord = SwapRecord(
    swapHash = rs.getByteVector32FromHex("swap_hash"),
    swapType = SwapType.fromName(rs.getString("swap_type")),
    state = rs.getString("state"),
    amount = Satoshi(rs.getLong("amount_sat")),
    preimage = rs.getByteVector32FromHex("preimage"),
    keyIndex = rs.getLong("key_index"),
    cltvExpiry = BlockHeight(rs.getLong("cltv_expiry")),
    contract = rs.getByteVector("contract"),
    createdAt = TimestampMilli(rs.getTimestamp("created_at").getTime),
    updatedAt = TimestampMilli(rs.getTimestamp("updated_at").getTime),
    requestId_opt = for {
      key <- Option(rs.getString("idempotency_key"))
      hash <- rs.getByteVector32FromHexNullable("request_hash")
    } yield SwapRequestId(key, hash)
  )

  override def getSwap(swapHash: ByteVector32): Option[SwapRecord] = withLock { pg =>
    using(pg.prepareStatement("SELECT * FROM loop.swaps WHERE swap_hash=?")) { statement =>
      statement.setString(1, swapHash.toHex)
      statement.executeQuery().map(parseSwap).headOption
    }
  }

  override def listSwaps(): Seq[SwapRecord] = withLock { pg =>
    using(pg.prepareStatement("SELECT * FROM loop.swaps ORDER BY created_at")) { statement =>
      statement.executeQuery().map(parseSwap).toSeq
    }
  }

  override def getSwapByIdempotencyKey(idempotencyKey: String): Option[SwapRecord] = withLock { pg =>
    using(pg.prepareStatement("SELECT * FROM loop.swaps WHERE idempotency_key=?")) { statement =>
      statement.setString(1, idempotencyKey)
      statement.executeQuery().map(parseSwap).headOption
    }
  }

  override def listSwapUpdates(swapHash: ByteVector32): Seq[SwapUpdate] = withLock { pg =>
    using(pg.prepareStatement("SELECT * FROM loop.swap_updates WHERE swap_hash=? ORDER BY id")) { statement =>
      statement.setString(1, swapHash.toHex)
      statement.executeQuery().map(rs => SwapUpdate(rs.getByteVector32FromHex("swap_hash"), rs.getString("state"), TimestampMilli(rs.getTimestamp("created_at").getTime))).toSeq
    }
  }

  override def nextKeyIndex(): Long = withLock { pg =>
    using(pg.prepareStatement("SELECT MAX(key_index) AS max_index FROM loop.swaps")) { statement =>
      statement.executeQuery().map(_.getLongNullable("max_index")).headOption.flatten.map(_ + 1).getOrElse(0L)
    }
  }

  private def parseToken(rs: ResultSet): L402Token = L402Token(
    paymentHash = rs.getByteVector32FromHex("payment_hash"),
    baseMacaroon = rs.getByteVector("base_macaroon"),
    preimage_opt = rs.getByteVector32FromHexNullable("preimage"),
    amountPaid = MilliSatoshi(rs.getLong("amount_paid_msat")),
    routingFeePaid = MilliSatoshi(rs.getLong("routing_fee_paid_msat")),
    createdAt = TimestampMilli(rs.getTimestamp("created_at").getTime)
  )

  private def upsertToken(pg: Connection, token: L402Token): Unit = {
    using(pg.prepareStatement(
      """INSERT INTO loop.l402_tokens VALUES (?, ?, ?, ?, ?, ?)
        |ON CONFLICT (payment_hash) DO UPDATE SET base_macaroon=EXCLUDED.base_macaroon, preimage=EXCLUDED.preimage, amount_paid_msat=EXCLUDED.amount_paid_msat, routing_fee_paid_msat=EXCLUDED.routing_fee_paid_msat, created_at=EXCLUDED.created_at""".stripMargin)) { statement =>
      statement.setString(1, token.paymentHash.toHex)
      statement.setBytes(2, token.baseMacaroon.toArray)
      statement.setString(3, token.preimage_opt.map(_.toHex).orNull)
      statement.setLong(4, token.amountPaid.toLong)
      statement.setLong(5, token.routingFeePaid.toLong)
      statement.setTimestamp(6, ts(token.createdAt))
      statement.executeUpdate()
    }
  }

  override def storeL402Token(token: L402Token): Unit = withLock { pg => upsertToken(pg, token) }

  override def currentL402Token(): Option[L402Token] = withLock { pg =>
    using(pg.prepareStatement("SELECT * FROM loop.l402_tokens ORDER BY created_at DESC LIMIT 1")) { statement =>
      statement.executeQuery().map(parseToken).headOption
    }
  }

  override def listL402Tokens(): Seq[L402Token] = withLock { pg =>
    using(pg.prepareStatement("SELECT * FROM loop.l402_tokens ORDER BY created_at")) { statement =>
      statement.executeQuery().map(parseToken).toSeq
    }
  }

  override def removePendingL402Token(paymentHash: ByteVector32): Unit = withLock { pg =>
    using(pg.prepareStatement("DELETE FROM loop.l402_tokens WHERE payment_hash=? AND preimage IS NULL")) { statement =>
      statement.setString(1, paymentHash.toHex)
      statement.executeUpdate()
    }
  }

  /**
   * Copies the swaps of the sqlite db that we used before eclair switched to postgres: we would otherwise stop running
   * the pending ones (a loop out would never sweep its htlc after revealing the preimage, and a loop in would never be
   * refunded). We then mark the sqlite db as migrated, so that we refuse to use it again if eclair switches back to
   * sqlite. If postgres already has other swaps, we can't merge them automatically: we fail, and eclair doesn't start.
   *
   * L402 tokens don't hold funds, so we always merge them: we copy those that postgres doesn't have, and keep its version
   * of the others (we used it since). The most recent token becomes our current token, and if it is pending we check its
   * payment as usual.
   *
   * When eclair always used postgres, we just created loop.sqlite: we only mark it, without creating our schema.
   */
  def migrateFromSqlite(sqlite: Connection): Unit = {
    if (!SqliteLoopDb.isMigratedToPostgres(sqlite)) {
      if (SqliteLoopDb.hasSchema(sqlite)) copyFromSqlite(new SqliteLoopDb(sqlite))
      SqliteLoopDb.markMigratedToPostgres(sqlite)
    }
  }

  private def copyFromSqlite(from: SqliteLoopDb): Unit = {
    val swaps = from.listSwaps()
    val existingSwaps = listSwaps().map(_.swapHash).toSet
    val swapsToCopy = if (swaps.forall(s => existingSwaps.contains(s.swapHash))) {
      // Either there are none, or we copied them in a single transaction but stopped before marking the sqlite db:
      // they may have been updated since.
      if (swaps.nonEmpty) logger.info("swaps of loop.sqlite were already migrated to postgres")
      Nil
    } else if (existingSwaps.isEmpty) {
      swaps
    } else {
      throw new RuntimeException(s"cannot migrate ${swaps.size} swaps from loop.sqlite to eclair's postgres database: its loop schema already has ${existingSwaps.size} other swaps. Pending swaps would never complete if we ignored either database: merge them manually, or move loop.sqlite away if none of its swaps is pending.")
    }
    val existingTokens = listL402Tokens().map(_.paymentHash).toSet
    val tokensToCopy = from.listL402Tokens().filterNot(t => existingTokens.contains(t.paymentHash))
    if (swapsToCopy.nonEmpty || tokensToCopy.nonEmpty) {
      val updates = swapsToCopy.map(swap => swap.swapHash -> from.listSwapUpdates(swap.swapHash)).toMap
      withLock { pg =>
        swapsToCopy.foreach { swap =>
          insertSwap(pg, swap)
          updates(swap.swapHash).foreach(u => insertUpdate(pg, u.swapHash, u.state, u.createdAt))
        }
        tokensToCopy.foreach(token => upsertToken(pg, token))
      }
      logger.info(s"migrated ${swapsToCopy.size} swaps and ${tokensToCopy.size} L402 tokens from loop.sqlite to postgres")
    }
  }

  override def close(): Unit = () // the data source is owned by eclair

}
