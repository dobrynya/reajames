package ru.reajames

import Jms._
import scala.util.{Failure, Try}
import javax.jms.{Connection, ConnectionFactory}

/**
  * Contains already established and started connection or failure.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 13:44.
  */
class ConnectionHolder(connectionFactory: => Connection, name: Option[String]) extends Logging {
  def this(connectionFactory: ConnectionFactory, name: Option[String] = None,
           credentials: Option[(String, String)] = None, clientId: Option[String] = None) =
    this(
      mutate {
        credentials.map {
          case (user, password) =>
            connectionFactory.createConnection(user, password)
        }.getOrElse(connectionFactory.createConnection())
      } (c => clientId.foreach(c.setClientID)),
      name
    )

  val connection: Try[Connection] =
    (for {
      connection <- Try(connectionFactory)
      _ <- start(connection)
    } yield {
      logger.debug("Connection {} has been successfully established and started", name.getOrElse(connection))
      connection
    }) recoverWith {
      case th =>
        logger.error("Connection {} could not be established!", name.getOrElse(""))
        Failure(th)
    }

  def release(): Try[Unit] =
    for {
      c <- connection
      _ <- close(c)
    } yield logger.debug("Connection {} has been closed", name.getOrElse(c))
}
