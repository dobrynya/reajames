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
class JmsSender[T](connectionHolder: ConnectionHolder,
                   messageFactory: DestinationAwareMessageFactory[T])
                  (implicit executionContext: ExecutionContext) extends Subscriber[T] with Logging {

  private[reajames] var state: Subscriber[T] = unsubscribed

  def onSubscribe(subscription: Subscription): Unit =
    if (subscription != null) state.onSubscribe(subscription)
    else throw new NullPointerException("Subscription should be specified!")

  def onNext(e: T): Unit = state.onNext(e)
  def onComplete(): Unit = state.onComplete()
  def onError(th: Throwable): Unit = state.onError(th)

  private def unsubscribed = Unsubscribed.asInstanceOf[Subscriber[T]]

  object Unsubscribed extends Subscriber[Any] {
    def onSubscribe(subscription: Subscription): Unit = Future {
      val connected = for {
        c <- connectionHolder.connection
        s <- session(c)
        p <- producer(s)
      } yield Subscribed(c, s, p, subscription)

      connected match {
        case Success(ctx) =>
          logger.debug("Successfully established connection {}", ctx.connection)
          state = ctx
          subscription.request(1)
        case Failure(th) =>
          logger.error(s"Could not establish connection using $connectionHolder!", th)
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

  case class Subscribed(connection: Connection, session: Session, producer: MessageProducer, subscription: Subscription)
    extends Subscriber[T] {

    def onNext(elem: T): Unit = Future {
      val (message, destination) = messageFactory(session, elem)

      send(producer, message, destination) match {
        case Success(msg) =>
          logger.debug("Sent {}", msg)
          subscription.request(1)
        case Failure(th) =>
          logger.warn(s"Could not send a message to $destination, closing producer!", th)
          subscription.cancel()
          state = unsubscribed
          close(producer).recover {
            case throwable => logger.warn("An error occurred during closing producer!", throwable)
          }
      }
    }

    def onError(th: Throwable): Unit = Future {
      logger.warn(s"An error occurred in the upstream, closing $connection!", th)
      state = unsubscribed
      close(producer).recover {
        case throwable => logger.warn("An error occurred during closing producer!", throwable)
      }
    }

    def onComplete(): Unit = Future {
      logger.debug("Upstream has been completed, closing {}", connection)
      state = unsubscribed
      close(producer).recover {
        case th => logger.warn("An error occurred when closing producer!", th)
      }
    }

    def onSubscribe(s: Subscription): Unit = s.cancel() // already subscribed
  }
}