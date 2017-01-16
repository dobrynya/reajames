package ru.reajames

import Jms._
import org.reactivestreams._
import scala.util.{Failure, Success}
import scala.concurrent.{ExecutionContext, Future}
import javax.jms.{Connection, ConnectionFactory, MessageProducer, Session}

/**
  * Represents a subscriber in terms of reactive streams. It provides ability to connect a JMS destination and
  * listen to messages.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 3:49.
  */
class JmsSender[T](connectionFactory: ConnectionFactory,
                   destinationFactory: DestinationFactory,
                   messageFactory: MessageFactory[T],
                   credentials: Option[(String, String)] = None)
                  (implicit executionContext: ExecutionContext) extends Subscriber[T] with Logging {
  var state: Subscriber[T] = Unsubscribed.asInstanceOf[Subscriber[T]]

  def onSubscribe(subscription: Subscription): Unit =
    if (subscription != null) state.onSubscribe(subscription)
    else throw new NullPointerException("Subscription should be specified!")

  def onNext(e: T): Unit = state.onNext(e)
  def onComplete(): Unit = state.onComplete()
  def onError(th: Throwable): Unit = state.onError(th)

  object Unsubscribed extends Subscriber[Any] {
    def onSubscribe(subscription: Subscription): Unit = Future {
      val connected = for {
        c <- connection(connectionFactory, credentials)
        s <- session(c)
        d <- destination(s, destinationFactory)
        p <- producer(s, d)
      } yield new Subscribed(c, s, p, subscription)

      connected match {
        case Success(ctx) =>
          logger.debug("Successfully connected to {} at {}", destinationFactory.asInstanceOf[Any], connectionFactory)
          state = ctx
          subscription.request(1)
        case Failure(th) =>
          logger.error("Could not establish connection to #destinationFactory at $connectionFactory!", th)
          subscription.cancel()
      }
    }

    def onError(th: Throwable): Unit =
      logger.warn("JmsSender is unsubscribed but onError has been received!", th)

    def onComplete(): Unit =
      logger.warn("JmsSender is unsubscribed but onComplete has been received!")

    def onNext(element: Any): Unit =
      logger.warn("JmsSender is unsubscribed but onNext({}) has been received!", element)
  }

  class Subscribed(connection: Connection, session: Session, producer: MessageProducer, subscription: Subscription)
    extends Subscriber[T] {

    def onNext(t: T): Unit = Future {
      send(producer, messageFactory(session)(t)) match {
        case Success(msg) =>
          logger.debug("Sent a message {}", msg)
          subscription.request(1)
        case Failure(th) =>
          logger.warn(s"Could not send a message to $destinationFactory at $connectionFactory, closing connection!", th)
          subscription.cancel()
          close(connection).recover {
            case throwable => logger.warn("An error occurred during closing connection!", throwable)
          }
      }
    }

    def onError(th: Throwable): Unit = Future {
      logger.warn(s"An error occurred in the upstream, closing $destinationFactory at $connectionFactory!", th)
      close(connection).recover {
        case throwable => logger.warn("An error occurred during closing connection!", throwable)
      }
    }

    def onComplete(): Unit = Future {
      logger.debug("Upstream is completed, closing {} at {}", destinationFactory.asInstanceOf[Any], connectionFactory)
      close(connection).recover {
        case th => logger.warn("An error occurred during closing connection!", th)
      }
    }

    def onSubscribe(s: Subscription): Unit = s.cancel() // already subscribed
  }
}