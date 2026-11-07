package io.github.rorp.sluice

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import ch.qos.logback.{classic => logback}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.ListHasAsScala

object LogCapture {

  /**
   * Runs `f` with a function returning what the given class logged so far.
   *
   * Suites run in parallel: slf4j may be initializing in another suite's thread. It then gives us a placeholder logger,
   * and once initialized replays what was logged in the meantime, which we would capture too. slf4j (2.0) initializes and
   * replays while holding its class lock: we take it to wait until that's done.
   */
  def withLogs[T](c: Class[_])(f: (() => Seq[ILoggingEvent]) => T): T = {
    val logger = classOf[LoggerFactory].synchronized(LoggerFactory.getLogger(c)).asInstanceOf[logback.Logger]
    val appender = new ListAppender[ILoggingEvent]()
    appender.start()
    logger.addAppender(appender)
    try f(() => appender.list.asScala.toSeq) finally logger.detachAppender(appender)
  }

}
