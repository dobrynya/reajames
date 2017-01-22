package ru.reajames

import Jms._
import scala.util.{Failure, Try}
import javax.jms.{Connection, ConnectionFactory}

/**
  * Contains already established and started connection or failure.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 13:44.
  */
class ConnectionHolder(connectionFactory: => Connection) extends Logging {
  def this(connectionFactory: ConnectionFactory,
           credentials: Option[(String, String)] = None, clientId: Option[String] = None) =
    this {
      mutate {
        credentials.map {
          case (user, password) =>
            connectionFactory.createConnection(user, password)
        }.getOrElse(connectionFactory.createConnection())
      } (c => clientId.foreach(c.setClientID))
    }

  val connection: Try[Connection] =
    (for {
      connection <- Try(connectionFactory)
      _ <- start(connection)
    } yield {
      logger.debug("Connection {} has been successfully established and started", connection)
      connection
    }) recoverWith {
      case th =>
        logger.error("Connection could not be established!")
        Failure(th)
    }

  def release(): Try[Unit] =
    for {
      c <- connection
      _ <- close(c)
    } yield logger.debug("Connection {} has been closed", c)
}
