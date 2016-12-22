package ru.reajames

import org.slf4j._

/**
  * Mixes logging ability.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 1:23.
  */
trait Logging {
  val logger: Logger = LoggerFactory.getLogger(getClass)
}
