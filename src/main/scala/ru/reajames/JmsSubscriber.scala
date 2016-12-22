package ru.reajames

import Jms._
import org.reactivestreams._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext
import javax.jms.{Connection, ConnectionFactory, MessageProducer, Session}

/**
  * Represents a subscriber in terms of reactive streams. It provides ability to connect a JMS destination and
  * listen to messages.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 3:49.
  */
class JmsSubscriber[T](connectionFactory: ConnectionFactory,
                       destinationFactory: DestinationFactory,
                       messageFactory: MessageFactory[T],
                       request: Long = 1000)
                      (implicit executionContext: ExecutionContext) extends Subscriber[T] with Logging {
  var context: Context = _

  def onError(th: Throwable): Unit = {
    logger.warn(s"An error occurred in the upstream, closing $destinationFactory at $connectionFactory!", th)
    if (context != null) close(context.connection)
  }

  def onSubscribe(subscription: Subscription): Unit = {
    if (subscription == null)
      throw new NullPointerException("Subscription should be specified!")

    val connected = for {
      c <- connection(connectionFactory)
      s <- session(c)
      d <- destination(s, destinationFactory)
      p <- producer(s, d)
    } yield Context(c, s, p, subscription)

    connected match {
      case Success(ctx) =>
        logger.debug("Successfully connected to {} at {}", destinationFactory.asInstanceOf[Any], connectionFactory)
        context = ctx
        subscription.request(Long.MaxValue)
      case Failure(th) =>
        logger.error("Could not establish connection to #destinationFactory at $connectionFactory!", th)
        subscription.cancel()
    }
  }

  def onComplete(): Unit = {
    logger.debug("Upstream is completed, closing {} at {}", destinationFactory.asInstanceOf[Any], connectionFactory)
    if (context != null) close(context.connection)
  }

  def onNext(e: T): Unit = {
    if (context != null)
      send(context.producer, messageFactory(context.session)(e)) match {
        case Success(msg) =>
          logger.debug("Sent a message {}", msg)
        case Failure(th) =>
          logger.warn(s"Could not send a message to $destinationFactory at $connectionFactory, closing connection!", th)
          close(context.connection)
      }
  }

  case class Context(connection: Connection, session: Session, producer: MessageProducer, subscription: Subscription)
}