package io.github.rorp.sluice.db

import fr.acinq.bitcoin.scalacompat.{ByteVector32, SatoshiLong}
import fr.acinq.eclair.db.jdbc.JdbcUtils
import fr.acinq.eclair.db.pg.PgUtils.PgLock
import io.github.rorp.sluice.SluicePlugin
import fr.acinq.eclair.{BlockHeight, MilliSatoshi, TimestampMilli, randomBytes, randomBytes32}
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files
import java.sql.{Connection, DriverManager, Timestamp}
import java.util.Comparator
import javax.sql.DataSource
import scala.concurrent.duration.DurationInt

object LoopDbSpec {
  private lazy val pg = EmbeddedPostgres.start()

  def newPgDataSource(): DataSource = {
    implicit val ds: DataSource = pg.getPostgresDatabase
    val dbName = s"db_${randomBytes(8).toHex}"
    JdbcUtils.withConnection(_.createStatement().executeUpdate(s"CREATE DATABASE $dbName"))
    pg.getDatabase("postgres", dbName)
  }
}

class LoopDbSpec extends AnyFunSuite {

  private def forAllDbs(f: (() => LoopDb) => Unit): Unit = {
    // sqlite: re-opening the db means re-using the same connection
    val sqlite = DriverManager.getConnection("jdbc:sqlite::memory:")
    try f(() => new SqliteLoopDb(sqlite)) finally sqlite.close()
    val ds = LoopDbSpec.newPgDataSource()
    f(() => new PgLoopDb()(ds, PgLock.NoLock))
  }

  // Postgres timestamps have microsecond precision, so we use round values.
  private def now(): TimestampMilli = TimestampMilli((TimestampMilli.now().toLong / 1000) * 1000)

  private def newSwap(swapType: SwapType, keyIndex: Long, createdAt: TimestampMilli): SwapRecord =
    SwapRecord(randomBytes32(), swapType, "initiated", 500_000 sat, randomBytes32(), keyIndex, BlockHeight(800_100), randomBytes(150), createdAt, createdAt)

  test("init db (idempotent)") {
    forAllDbs { open =>
      val db = open()
      open() // opening the db again doesn't recreate the tables
      assert(db.listSwaps().isEmpty)
      assert(db.nextKeyIndex() == 0)
    }
  }

  test("add, update and list swaps") {
    forAllDbs { open =>
      val db = open()
      val t0 = now()
      val swap1 = newSwap(SwapType.LoopOut, db.nextKeyIndex(), t0)
      db.addSwap(swap1)
      assert(db.nextKeyIndex() == 1)
      val swap2 = newSwap(SwapType.LoopIn, db.nextKeyIndex(), t0 + 1.second)
      db.addSwap(swap2)
      assert(db.nextKeyIndex() == 2)
      assert(db.getSwap(swap1.swapHash).contains(swap1))
      assert(db.getSwap(randomBytes32()).isEmpty)
      assert(db.listSwaps() == Seq(swap1, swap2))
      // duplicate swap hash or key index
      assertThrows[Exception](db.addSwap(swap1.copy(keyIndex = 42)))
      assertThrows[Exception](db.addSwap(newSwap(SwapType.LoopOut, swap1.keyIndex, t0)))
      assert(db.listSwaps().size == 2)

      val contract = randomBytes(200)
      db.updateSwap(swap1.swapHash, "htlc_published", contract, t0 + 2.seconds)
      db.updateSwap(swap1.swapHash, "success", contract, t0 + 3.seconds)
      assert(db.getSwap(swap1.swapHash).contains(swap1.copy(state = "success", contract = contract, updatedAt = t0 + 3.seconds)))
      assert(db.listSwapUpdates(swap1.swapHash) == Seq(
        SwapUpdate(swap1.swapHash, "initiated", t0),
        SwapUpdate(swap1.swapHash, "htlc_published", t0 + 2.seconds),
        SwapUpdate(swap1.swapHash, "success", t0 + 3.seconds),
      ))
      assert(db.listSwapUpdates(swap2.swapHash) == Seq(SwapUpdate(swap2.swapHash, "initiated", t0 + 1.second)))
      assertThrows[IllegalArgumentException](db.updateSwap(randomBytes32(), "success", contract, t0))
      // data is persisted
      assert(open().listSwaps() == db.listSwaps())
    }
  }

  test("find swaps by idempotency key") {
    forAllDbs { open =>
      val db = open()
      val requestId = SwapRequestId("payout-42", randomBytes32())
      val swap1 = newSwap(SwapType.LoopOut, 0, now()).copy(requestId_opt = Some(requestId))
      db.addSwap(swap1)
      // Swaps without a key don't conflict.
      val swap2 = newSwap(SwapType.LoopIn, 1, now())
      val swap3 = newSwap(SwapType.LoopIn, 2, now())
      db.addSwap(swap2)
      db.addSwap(swap3)
      assert(db.getSwapByIdempotencyKey("payout-42").contains(swap1))
      assert(db.getSwapByIdempotencyKey("payout-43").isEmpty)
      // A key belongs to one swap at most.
      assertThrows[Exception](db.addSwap(newSwap(SwapType.LoopOut, 3, now()).copy(requestId_opt = Some(requestId.copy(requestHash = randomBytes32())))))
      assert(db.listSwaps().size == 3)
      assert(open().getSwapByIdempotencyKey("payout-42").contains(swap1))
    }
  }

  test("migrate the db from v1 (swaps without idempotency keys)") {
    val swap = newSwap(SwapType.LoopOut, 0, now())
    val keyed = newSwap(SwapType.LoopIn, 1, now()).copy(requestId_opt = Some(SwapRequestId("deposit-7", randomBytes32())))
    withSqlite { conn =>
      JdbcUtils.using(conn.createStatement()) { statement =>
        statement.executeUpdate("CREATE TABLE swaps (swap_hash BLOB NOT NULL PRIMARY KEY, swap_type TEXT NOT NULL, state TEXT NOT NULL, amount_sat INTEGER NOT NULL, preimage BLOB NOT NULL, key_index INTEGER NOT NULL UNIQUE, cltv_expiry INTEGER NOT NULL, contract BLOB NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)")
        statement.executeUpdate("CREATE TABLE swap_updates (id INTEGER PRIMARY KEY AUTOINCREMENT, swap_hash BLOB NOT NULL REFERENCES swaps(swap_hash), state TEXT NOT NULL, created_at INTEGER NOT NULL)")
        statement.executeUpdate("CREATE TABLE l402_tokens (payment_hash BLOB NOT NULL PRIMARY KEY, base_macaroon BLOB NOT NULL, preimage BLOB, amount_paid_msat INTEGER NOT NULL, routing_fee_paid_msat INTEGER NOT NULL, created_at INTEGER NOT NULL)")
        statement.executeUpdate("CREATE INDEX swap_updates_swap_hash_idx ON swap_updates(swap_hash)")
        statement.executeUpdate("CREATE INDEX swaps_state_idx ON swaps(state)")
        JdbcUtils.setVersion(statement, SqliteLoopDb.DB_NAME, 1)
      }
      JdbcUtils.using(conn.prepareStatement("INSERT INTO swaps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) { statement =>
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
        statement.executeUpdate()
      }
      val db = new SqliteLoopDb(conn)
      assert(JdbcUtils.using(conn.createStatement())(JdbcUtils.getVersion(_, SqliteLoopDb.DB_NAME)).contains(SqliteLoopDb.CURRENT_VERSION))
      assert(db.listSwaps() == Seq(swap))
      db.addSwap(keyed)
      assert(new SqliteLoopDb(conn).getSwapByIdempotencyKey("deposit-7").contains(keyed))
      assertThrows[Exception](db.addSwap(newSwap(SwapType.LoopIn, 2, now()).copy(requestId_opt = keyed.requestId_opt)))
    }
    implicit val ds: DataSource = LoopDbSpec.newPgDataSource()
    JdbcUtils.withConnection { conn =>
      JdbcUtils.using(conn.createStatement()) { statement =>
        statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS loop")
        statement.executeUpdate("CREATE TABLE loop.swaps (swap_hash TEXT NOT NULL PRIMARY KEY, swap_type TEXT NOT NULL, state TEXT NOT NULL, amount_sat BIGINT NOT NULL, preimage TEXT NOT NULL, key_index BIGINT NOT NULL UNIQUE, cltv_expiry BIGINT NOT NULL, contract BYTEA NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL, updated_at TIMESTAMP WITH TIME ZONE NOT NULL)")
        statement.executeUpdate("CREATE TABLE loop.swap_updates (id BIGSERIAL PRIMARY KEY, swap_hash TEXT NOT NULL REFERENCES loop.swaps(swap_hash), state TEXT NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL)")
        statement.executeUpdate("CREATE TABLE loop.l402_tokens (payment_hash TEXT NOT NULL PRIMARY KEY, base_macaroon BYTEA NOT NULL, preimage TEXT, amount_paid_msat BIGINT NOT NULL, routing_fee_paid_msat BIGINT NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL)")
        statement.executeUpdate("CREATE INDEX swap_updates_swap_hash_idx ON loop.swap_updates(swap_hash)")
        statement.executeUpdate("CREATE INDEX swaps_state_idx ON loop.swaps(state)")
        JdbcUtils.setVersion(statement, PgLoopDb.DB_NAME, 1)
      }
      JdbcUtils.using(conn.prepareStatement("INSERT INTO loop.swaps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) { statement =>
        statement.setString(1, swap.swapHash.toHex)
        statement.setString(2, swap.swapType.name)
        statement.setString(3, swap.state)
        statement.setLong(4, swap.amount.toLong)
        statement.setString(5, swap.preimage.toHex)
        statement.setLong(6, swap.keyIndex)
        statement.setLong(7, swap.cltvExpiry.toLong)
        statement.setBytes(8, swap.contract.toArray)
        statement.setTimestamp(9, new Timestamp(swap.createdAt.toLong))
        statement.setTimestamp(10, new Timestamp(swap.updatedAt.toLong))
        statement.executeUpdate()
      }
    }
    val pg = new PgLoopDb()(ds, PgLock.NoLock)
    assert(JdbcUtils.withConnection(conn => JdbcUtils.using(conn.createStatement())(JdbcUtils.getVersion(_, PgLoopDb.DB_NAME))).contains(PgLoopDb.CURRENT_VERSION))
    assert(pg.listSwaps() == Seq(swap))
    pg.addSwap(keyed)
    assert(new PgLoopDb()(ds, PgLock.NoLock).getSwapByIdempotencyKey("deposit-7").contains(keyed))
    assertThrows[Exception](pg.addSwap(newSwap(SwapType.LoopIn, 2, now()).copy(requestId_opt = keyed.requestId_opt)))
  }

  test("store L402 tokens") {
    forAllDbs { open =>
      val db = open()
      assert(db.currentL402Token().isEmpty)
      val t0 = now()
      val token1 = L402Token(randomBytes32(), randomBytes(100), Some(randomBytes32()), MilliSatoshi(1_000_000), MilliSatoshi(1000), t0)
      db.storeL402Token(token1)
      assert(db.currentL402Token().contains(token1))
      val token2 = L402Token(randomBytes32(), randomBytes(100), None, MilliSatoshi(0), MilliSatoshi(0), t0 + 1.second)
      db.storeL402Token(token2)
      assert(db.currentL402Token().contains(token2))
      assert(db.listL402Tokens() == Seq(token1, token2))
      // paid tokens can't be removed
      db.removePendingL402Token(token1.paymentHash)
      assert(db.listL402Tokens() == Seq(token1, token2))
      // the pending token is updated once paid
      val token2Paid = token2.copy(preimage_opt = Some(ByteVector32.One), amountPaid = MilliSatoshi(1_000_000), routingFeePaid = MilliSatoshi(500))
      db.storeL402Token(token2Paid)
      assert(db.listL402Tokens() == Seq(token1, token2Paid))
      // pending tokens can be removed
      val token3 = L402Token(randomBytes32(), randomBytes(100), None, MilliSatoshi(0), MilliSatoshi(0), t0 + 2.seconds)
      db.storeL402Token(token3)
      db.removePendingL402Token(token3.paymentHash)
      assert(db.currentL402Token().contains(token2Paid))
    }
  }

  private def withSqlite(f: Connection => Unit): Unit = {
    val sqlite = DriverManager.getConnection("jdbc:sqlite::memory:")
    try f(sqlite) finally sqlite.close()
  }

  private def newPgDb(): PgLoopDb = new PgLoopDb()(LoopDbSpec.newPgDataSource(), PgLock.NoLock)

  test("migrate swaps from sqlite to postgres") {
    withSqlite { conn =>
      val sqlite = new SqliteLoopDb(conn)
      val t0 = now()
      val swap1 = newSwap(SwapType.LoopOut, 0, t0)
      val swap2 = newSwap(SwapType.LoopIn, 1, t0 + 1.second).copy(requestId_opt = Some(SwapRequestId("deposit-7", randomBytes32())))
      sqlite.addSwap(swap1)
      sqlite.addSwap(swap2)
      sqlite.updateSwap(swap1.swapHash, "preimage_revealed", randomBytes(200), t0 + 2.seconds)
      val token1 = L402Token(randomBytes32(), randomBytes(100), Some(randomBytes32()), MilliSatoshi(1_000_000), MilliSatoshi(1000), t0)
      val token2 = L402Token(randomBytes32(), randomBytes(100), None, MilliSatoshi(0), MilliSatoshi(0), t0 + 1.second)
      sqlite.storeL402Token(token1)
      sqlite.storeL402Token(token2)
      val pg = newPgDb()
      pg.migrateFromSqlite(conn)
      assert(pg.listSwaps() == sqlite.listSwaps())
      assert(pg.getSwap(swap1.swapHash).map(_.state).contains("preimage_revealed"))
      // Retries of the request that created a swap still find it.
      assert(pg.getSwapByIdempotencyKey("deposit-7").contains(swap2))
      Seq(swap1, swap2).foreach(swap => assert(pg.listSwapUpdates(swap.swapHash) == sqlite.listSwapUpdates(swap.swapHash)))
      assert(pg.listSwapUpdates(swap1.swapHash).size == 2)
      assert(pg.listL402Tokens() == Seq(token1, token2))
      assert(pg.nextKeyIndex() == 2)
      // Our swaps continue in postgres: we never migrate them again, and we refuse to use the sqlite db.
      pg.updateSwap(swap1.swapHash, "success", randomBytes(200), t0 + 3.seconds)
      val migrated = pg.listSwaps()
      pg.migrateFromSqlite(conn)
      assert(pg.listSwaps() == migrated)
      assert(intercept[RuntimeException](new SqliteLoopDb(conn)).getMessage.contains("loop.sqlite is marked as replaced by eclair's postgres database"))
    }
  }

  test("mark an empty sqlite db as migrated to postgres") {
    withSqlite { conn =>
      // That's the case when eclair always used postgres: we just created loop.sqlite.
      val pg = newPgDb()
      pg.migrateFromSqlite(conn)
      assert(SqliteLoopDb.isMigratedToPostgres(conn))
      assert(pg.listSwaps().isEmpty && pg.listL402Tokens().isEmpty)
      // It only holds the mark: we didn't create our schema.
      assert(!SqliteLoopDb.hasSchema(conn))
      assert(sqliteTables(conn) == Set("versions"))
      // If eclair switches to sqlite, we don't silently start without the swaps stored in postgres.
      assert(intercept[RuntimeException](new SqliteLoopDb(conn)).getMessage.contains("loop.sqlite is marked as replaced by eclair's postgres database"))
      assert(sqliteTables(conn) == Set("versions"))
    }
  }

  test("mark an sqlite db with an empty schema as migrated to postgres") {
    withSqlite { conn =>
      // loop.sqlite was created by an earlier version, which created our schema before marking it.
      new SqliteLoopDb(conn)
      val pg = newPgDb()
      pg.migrateFromSqlite(conn)
      assert(SqliteLoopDb.isMigratedToPostgres(conn))
      assert(pg.listSwaps().isEmpty && pg.listL402Tokens().isEmpty)
    }
  }

  private def sqliteTables(conn: Connection): Set[String] = {
    val rs = conn.createStatement().executeQuery("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
    Iterator.continually(rs).takeWhile(_.next()).map(_.getString("name")).toSet
  }

  test("refuse to migrate sqlite swaps when postgres has other swaps") {
    withSqlite { conn =>
      val sqlite = new SqliteLoopDb(conn)
      val swap1 = newSwap(SwapType.LoopIn, 0, now())
      sqlite.addSwap(swap1)
      val pg = newPgDb()
      val swap2 = newSwap(SwapType.LoopOut, 5, now())
      pg.addSwap(swap2)
      val token = L402Token(randomBytes32(), randomBytes(100), Some(randomBytes32()), MilliSatoshi(1_000_000), MilliSatoshi(1000), now())
      sqlite.storeL402Token(token)
      assert(intercept[RuntimeException](pg.migrateFromSqlite(conn)).getMessage.contains("cannot migrate 1 swaps from loop.sqlite to eclair's postgres database: its loop schema already has 1 other swaps"))
      // We copied nothing, not even tokens.
      assert(pg.listSwaps() == Seq(swap2))
      assert(pg.listL402Tokens().isEmpty)
      // The sqlite db still has its swaps, and isn't marked.
      assert(!SqliteLoopDb.isMigratedToPostgres(conn))
      assert(new SqliteLoopDb(conn).listSwaps() == Seq(swap1))
    }
  }

  test("merge L402 tokens when migrating from sqlite to postgres") {
    withSqlite { conn =>
      val sqlite = new SqliteLoopDb(conn)
      val t0 = now()
      val swap = newSwap(SwapType.LoopOut, 0, t0)
      sqlite.addSwap(swap)
      val sqliteToken = L402Token(randomBytes32(), randomBytes(100), Some(randomBytes32()), MilliSatoshi(1_000_000), MilliSatoshi(1000), t0)
      sqlite.storeL402Token(sqliteToken)
      // We paid for a token in postgres before migrating, for example because eclair failed to start on a previous attempt.
      val sharedToken = L402Token(randomBytes32(), randomBytes(100), None, MilliSatoshi(0), MilliSatoshi(0), t0 + 1.second)
      sqlite.storeL402Token(sharedToken)
      val pg = newPgDb()
      val pgToken = L402Token(randomBytes32(), randomBytes(100), Some(randomBytes32()), MilliSatoshi(2_000_000), MilliSatoshi(2000), t0 + 2.seconds)
      val paidSharedToken = sharedToken.copy(preimage_opt = Some(randomBytes32()), amountPaid = MilliSatoshi(1_000_000))
      pg.storeL402Token(paidSharedToken)
      pg.storeL402Token(pgToken)
      pg.migrateFromSqlite(conn)
      assert(SqliteLoopDb.isMigratedToPostgres(conn))
      assert(pg.listSwaps() == Seq(swap))
      // We keep postgres' version of tokens that both dbs have, and the most recent token stays current.
      assert(pg.listL402Tokens() == Seq(sqliteToken, paidSharedToken, pgToken))
      assert(pg.currentL402Token().contains(pgToken))
    }
  }

  test("migrate L402 tokens to postgres when sqlite has no swaps") {
    withSqlite { conn =>
      val sqlite = new SqliteLoopDb(conn)
      val sqliteToken = L402Token(randomBytes32(), randomBytes(100), Some(randomBytes32()), MilliSatoshi(1_000_000), MilliSatoshi(1000), now())
      sqlite.storeL402Token(sqliteToken)
      val pg = newPgDb()
      val swap = newSwap(SwapType.LoopIn, 0, now())
      pg.addSwap(swap)
      pg.migrateFromSqlite(conn)
      assert(SqliteLoopDb.isMigratedToPostgres(conn))
      assert(pg.listSwaps() == Seq(swap))
      assert(pg.listL402Tokens() == Seq(sqliteToken))
    }
  }

  test("finish a migration to postgres that stopped before marking the sqlite db") {
    withSqlite { conn =>
      val sqlite = new SqliteLoopDb(conn)
      val swap = newSwap(SwapType.LoopIn, 0, now())
      sqlite.addSwap(swap)
      // Postgres already has the swap (we copy all of them in a single transaction), maybe with a more recent state.
      val pg = newPgDb()
      pg.addSwap(swap)
      pg.updateSwap(swap.swapHash, "htlc_published", randomBytes(100), swap.createdAt + 1.second)
      pg.migrateFromSqlite(conn)
      assert(SqliteLoopDb.isMigratedToPostgres(conn))
      assert(pg.listSwaps().map(_.state) == Seq("htlc_published"))
      assert(pg.listSwapUpdates(swap.swapHash).size == 2)
    }
  }

  test("eclair switches from sqlite to postgres, and back") {
    val chaindir = Files.createTempDirectory("sluice-db").toFile
    try {
      val swap = newSwap(SwapType.LoopOut, 0, now())
      val db1 = SluicePlugin.openDb(chaindir, None)
      db1.addSwap(swap)
      db1.close()
      // Our swap continues in postgres.
      val ds = LoopDbSpec.newPgDataSource()
      assert(SluicePlugin.openDb(chaindir, Some(ds, PgLock.NoLock)).listSwaps() == Seq(swap))
      assert(SluicePlugin.openDb(chaindir, Some(ds, PgLock.NoLock)).listSwaps() == Seq(swap))
      // We don't start without it if eclair switches back to sqlite.
      assert(intercept[RuntimeException](SluicePlugin.openDb(chaindir, None)).getMessage.contains("loop.sqlite is marked as replaced by eclair's postgres database"))
      // If eclair always used postgres, we create a marked loop.sqlite, without our schema.
      val chaindir2 = new File(chaindir, "always-postgres")
      chaindir2.mkdirs() // eclair creates it
      val ds2 = LoopDbSpec.newPgDataSource()
      val swap2 = newSwap(SwapType.LoopIn, 0, now())
      SluicePlugin.openDb(chaindir2, Some(ds2, PgLock.NoLock)).addSwap(swap2)
      assert(SluicePlugin.openDb(chaindir2, Some(ds2, PgLock.NoLock)).listSwaps() == Seq(swap2))
      val conn2 = DriverManager.getConnection(s"jdbc:sqlite:${new File(chaindir2, "loop.sqlite").getPath}")
      try assert(sqliteTables(conn2) == Set("versions")) finally conn2.close()
      assert(intercept[RuntimeException](SluicePlugin.openDb(chaindir2, None)).getMessage.contains("loop.sqlite is marked as replaced by eclair's postgres database"))
    } finally {
      Files.walk(chaindir.toPath).sorted(Comparator.reverseOrder()).forEach(p => Files.delete(p))
    }
  }

}
