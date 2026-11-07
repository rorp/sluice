package io.github.rorp.sluice.db

import fr.acinq.bitcoin.scalacompat.{ByteVector32, Satoshi}
import fr.acinq.eclair.db.sqlite.SqliteUtils
import fr.acinq.eclair.{BlockHeight, MilliSatoshi, TimestampMilli}
import grizzled.slf4j.Logging

import java.sql.{Connection, ResultSet, Statement}

object SqliteLoopDb {

  import SqliteUtils._

  val DB_NAME = "loop"
  val CURRENT_VERSION = 2

  /**
   * Set once we copied the db to eclair's postgres database, or when we created it only to hold this mark because eclair
   * always used postgres (see [[PgLoopDb.migrateFromSqlite]]).
   */
  private val MIGRATED_DB_NAME = "loop_migrated_to_postgres"

  def isMigratedToPostgres(sqlite: Connection): Boolean = using(sqlite.createStatement()) { statement =>
    getVersion(statement, MIGRATED_DB_NAME).nonEmpty
  }

  /** False for a loop.sqlite that we just created: it has no swaps nor L402 tokens. */
  def hasSchema(sqlite: Connection): Boolean = using(sqlite.createStatement()) { statement =>
    getVersion(statement, DB_NAME).nonEmpty
  }

  /** We must never use the db again: its swaps continue in postgres. */
  def markMigratedToPostgres(sqlite: Connection): Unit = using(sqlite.createStatement(), inTransaction = true) { statement =>
    setVersion(statement, MIGRATED_DB_NAME, 1)
  }

}

/** The loop db lives in its own sqlite file, next to eclair's databases. */
class SqliteLoopDb(val sqlite: Connection) extends LoopDb with Logging {

  import SqliteLoopDb._
  import SqliteUtils.ExtendedResultSet._
  import SqliteUtils._

  if (isMigratedToPostgres(sqlite)) {
    throw new RuntimeException("loop.sqlite is marked as replaced by eclair's postgres database: our swaps are in its loop schema (including any migrated from loop.sqlite), and loop.sqlite doesn't have their current state. If eclair switched back to sqlite on purpose and none of the swaps in postgres is pending, delete loop.sqlite to start with an empty database.")
  }

  /** v2 records the idempotency key of the request that created a swap (several swaps may have none). */
  private def migration12(statement: Statement): Unit = {
    statement.executeUpdate("ALTER TABLE swaps ADD COLUMN idempotency_key TEXT")
    statement.executeUpdate("ALTER TABLE swaps ADD COLUMN request_hash BLOB")
    statement.executeUpdate("CREATE UNIQUE INDEX swaps_idempotency_key_idx ON swaps(idempotency_key)")
  }

  using(sqlite.createStatement(), inTransaction = true) { statement =>
    getVersion(statement, DB_NAME) match {
      case None =>
        statement.executeUpdate("CREATE TABLE swaps (swap_hash BLOB NOT NULL PRIMARY KEY, swap_type TEXT NOT NULL, state TEXT NOT NULL, amount_sat INTEGER NOT NULL, preimage BLOB NOT NULL, key_index INTEGER NOT NULL UNIQUE, cltv_expiry INTEGER NOT NULL, contract BLOB NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, idempotency_key TEXT, request_hash BLOB)")
        statement.executeUpdate("CREATE TABLE swap_updates (id INTEGER PRIMARY KEY AUTOINCREMENT, swap_hash BLOB NOT NULL REFERENCES swaps(swap_hash), state TEXT NOT NULL, created_at INTEGER NOT NULL)")
        statement.executeUpdate("CREATE TABLE l402_tokens (payment_hash BLOB NOT NULL PRIMARY KEY, base_macaroon BLOB NOT NULL, preimage BLOB, amount_paid_msat INTEGER NOT NULL, routing_fee_paid_msat INTEGER NOT NULL, created_at INTEGER NOT NULL)")
        statement.executeUpdate("CREATE INDEX swap_updates_swap_hash_idx ON swap_updates(swap_hash)")
        statement.executeUpdate("CREATE INDEX swaps_state_idx ON swaps(state)")
        statement.executeUpdate("CREATE UNIQUE INDEX swaps_idempotency_key_idx ON swaps(idempotency_key)")
      case Some(v@1) =>
        logger.warn(s"migrating db $DB_NAME, found version=$v current=$CURRENT_VERSION")
        migration12(statement)
      case Some(CURRENT_VERSION) => () // table is up-to-date, nothing to do
      case Some(unknownVersion) => throw new RuntimeException(s"Unknown version of DB $DB_NAME found, version=$unknownVersion")
    }
    setVersion(statement, DB_NAME, CURRENT_VERSION)
  }

  private def inTransaction[T](f: => T): T = synchronized {
    val autoCommit = sqlite.getAutoCommit
    sqlite.setAutoCommit(false)
    try {
      val res = f
      sqlite.commit()
      res
    } catch {
      case t: Throwable =>
        sqlite.rollback()
        throw t
    } finally {
      sqlite.setAutoCommit(autoCommit)
    }
  }

  private def insertUpdate(swapHash: ByteVector32, state: String, createdAt: TimestampMilli): Unit = {
    using(sqlite.prepareStatement("INSERT INTO swap_updates (swap_hash, state, created_at) VALUES (?, ?, ?)")) { statement =>
      statement.setBytes(1, swapHash.toArray)
      statement.setString(2, state)
      statement.setLong(3, createdAt.toLong)
      statement.executeUpdate()
    }
  }

  override def addSwap(swap: SwapRecord): Unit = inTransaction {
    using(sqlite.prepareStatement("INSERT INTO swaps (swap_hash, swap_type, state, amount_sat, preimage, key_index, cltv_expiry, contract, created_at, updated_at, idempotency_key, request_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) { statement =>
      statement.setBytes(1, swap.swapHash.toArray)
      statement.setString(2, swap.swapType.name)
      statement.setString(3, swap.state)
      statement.setLong(4, swap.amount.toLong)
      statement.setBytes(5, swap.preimage.toArray)
      statement.setLong(6, swap.keyIndex)
      statement.setLong(7, swap.cltvExpiry.toLong)
      statement.setBytes(8, swap.contract.toArray)
      statement.setLong(9, swap.createdAt.toLong)
      statement.setLong(10, swap.updatedAt.toLong)
      statement.setString(11, swap.requestId_opt.map(_.idempotencyKey).orNull)
      statement.setBytes(12, swap.requestId_opt.map(_.requestHash.toArray).orNull)
      statement.executeUpdate()
    }
    insertUpdate(swap.swapHash, swap.state, swap.createdAt)
  }

  override def updateSwap(swapHash: ByteVector32, state: String, contract: scodec.bits.ByteVector, updatedAt: TimestampMilli): Unit = inTransaction {
    val updated = using(sqlite.prepareStatement("UPDATE swaps SET state=?, contract=?, updated_at=? WHERE swap_hash=?")) { statement =>
      statement.setString(1, state)
      statement.setBytes(2, contract.toArray)
      statement.setLong(3, updatedAt.toLong)
      statement.setBytes(4, swapHash.toArray)
      statement.executeUpdate()
    }
    if (updated != 1) throw new IllegalArgumentException(s"unknown swap $swapHash")
    insertUpdate(swapHash, state, updatedAt)
  }

  private def parseSwap(rs: ResultSet): SwapRecord = SwapRecord(
    swapHash = rs.getByteVector32("swap_hash"),
    swapType = SwapType.fromName(rs.getString("swap_type")),
    state = rs.getString("state"),
    amount = Satoshi(rs.getLong("amount_sat")),
    preimage = rs.getByteVector32("preimage"),
    keyIndex = rs.getLong("key_index"),
    cltvExpiry = BlockHeight(rs.getLong("cltv_expiry")),
    contract = rs.getByteVector("contract"),
    createdAt = TimestampMilli(rs.getLong("created_at")),
    updatedAt = TimestampMilli(rs.getLong("updated_at")),
    requestId_opt = for {
      key <- Option(rs.getString("idempotency_key"))
      hash <- rs.getByteVector32Nullable("request_hash")
    } yield SwapRequestId(key, hash)
  )

  override def getSwap(swapHash: ByteVector32): Option[SwapRecord] = synchronized {
    using(sqlite.prepareStatement("SELECT * FROM swaps WHERE swap_hash=?")) { statement =>
      statement.setBytes(1, swapHash.toArray)
      statement.executeQuery().map(parseSwap).headOption
    }
  }

  override def listSwaps(): Seq[SwapRecord] = synchronized {
    using(sqlite.prepareStatement("SELECT * FROM swaps ORDER BY created_at")) { statement =>
      statement.executeQuery().map(parseSwap).toSeq
    }
  }

  override def getSwapByIdempotencyKey(idempotencyKey: String): Option[SwapRecord] = synchronized {
    using(sqlite.prepareStatement("SELECT * FROM swaps WHERE idempotency_key=?")) { statement =>
      statement.setString(1, idempotencyKey)
      statement.executeQuery().map(parseSwap).headOption
    }
  }

  override def listSwapUpdates(swapHash: ByteVector32): Seq[SwapUpdate] = synchronized {
    using(sqlite.prepareStatement("SELECT * FROM swap_updates WHERE swap_hash=? ORDER BY id")) { statement =>
      statement.setBytes(1, swapHash.toArray)
      statement.executeQuery().map(rs => SwapUpdate(rs.getByteVector32("swap_hash"), rs.getString("state"), TimestampMilli(rs.getLong("created_at")))).toSeq
    }
  }

  override def nextKeyIndex(): Long = synchronized {
    using(sqlite.prepareStatement("SELECT MAX(key_index) AS max_index FROM swaps")) { statement =>
      statement.executeQuery().map(_.getLongNullable("max_index")).headOption.flatten.map(_ + 1).getOrElse(0L)
    }
  }

  private def parseToken(rs: ResultSet): L402Token = L402Token(
    paymentHash = rs.getByteVector32("payment_hash"),
    baseMacaroon = rs.getByteVector("base_macaroon"),
    preimage_opt = rs.getByteVector32Nullable("preimage"),
    amountPaid = MilliSatoshi(rs.getLong("amount_paid_msat")),
    routingFeePaid = MilliSatoshi(rs.getLong("routing_fee_paid_msat")),
    createdAt = TimestampMilli(rs.getLong("created_at"))
  )

  override def storeL402Token(token: L402Token): Unit = synchronized {
    using(sqlite.prepareStatement("INSERT OR REPLACE INTO l402_tokens VALUES (?, ?, ?, ?, ?, ?)")) { statement =>
      statement.setBytes(1, token.paymentHash.toArray)
      statement.setBytes(2, token.baseMacaroon.toArray)
      statement.setBytes(3, token.preimage_opt.map(_.toArray).orNull)
      statement.setLong(4, token.amountPaid.toLong)
      statement.setLong(5, token.routingFeePaid.toLong)
      statement.setLong(6, token.createdAt.toLong)
      statement.executeUpdate()
    }
  }

  override def currentL402Token(): Option[L402Token] = synchronized {
    using(sqlite.prepareStatement("SELECT * FROM l402_tokens ORDER BY created_at DESC LIMIT 1")) { statement =>
      statement.executeQuery().map(parseToken).headOption
    }
  }

  override def listL402Tokens(): Seq[L402Token] = synchronized {
    using(sqlite.prepareStatement("SELECT * FROM l402_tokens ORDER BY created_at")) { statement =>
      statement.executeQuery().map(parseToken).toSeq
    }
  }

  override def removePendingL402Token(paymentHash: ByteVector32): Unit = synchronized {
    using(sqlite.prepareStatement("DELETE FROM l402_tokens WHERE payment_hash=? AND preimage IS NULL")) { statement =>
      statement.setBytes(1, paymentHash.toArray)
      statement.executeUpdate()
    }
  }

  override def close(): Unit = sqlite.close()

}
