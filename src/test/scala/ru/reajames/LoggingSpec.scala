package ru.reajames

import org.scalatest._

/**
  * Specification on Logging.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 06.07.17 22:43.
  */
class LoggingSpec extends FlatSpec with Matchers with Logging {
  behavior of "Logging"

  it should "provide instantiated logger" in {
    logger shouldNot be (null)
  }

  it should "log an exception" in {
    log("Just a message")(new RuntimeException("Generated exception!"))
  }
}