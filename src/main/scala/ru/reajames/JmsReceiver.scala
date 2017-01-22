package ru.reajames

import Jms._
import javax.jms._
import org.reactivestreams._
import scala.annotation.tailrec
import scala.util.{Failure, Success}
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/**
  * Represents a publisher in terms of reactive streams.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 21.12.16 0:14.
  */
class JmsReceiver(connectionHolder: ConnectionHolder, destinationFactory: DestinationFactory)
                 (implicit executionContext: ExecutionContext) extends Publisher[Message] with Logging {

  def subscribe(subscriber: Subscriber[_ >: Message]): Unit = {
    if (subscriber == null)
      throw new NullPointerException("Subscriber should be specified!")

    Future {
      val subscription = for {
        c <- connectionHolder.connection
        s <- session(c)
        d <- destination(s, destinationFactory)
        consumer <- consumer(s, d)
      } yield new JmsSubscription(c, s, consumer, subscriber)

      subscription match {
        case Success(s) =>
          logger.debug("Subscribed to {}", destinationFactory)
          subscriber.onSubscribe(s)
        case Failure(th) =>
          logger.warn(s"Could not subscribe to $destinationFactory", th)
          subscriber.onError(th)
      }
    }

    class JmsSubscription(connection: Connection, session: Session, consumer: MessageConsumer,
                              subscriber: Subscriber[_ >: Message]) extends Subscription {
      val cancelled = new AtomicBoolean(false)
      val requested = new AtomicLong(0)

      def request(n: Long): Unit = {
        logger.debug("Requested {} from {}", n, destinationFactory)
        if (n <= 0)
          throw new IllegalArgumentException(s"Wrong requested items amount $n!")
        else if (requested.getAndAdd(n) == 0)
          executionContext.execute(() => receiveMessage())
      }

      def cancel(): Unit =
        if (cancelled.compareAndSet(false, true)) {
          if (requested.get() == 0) subscriber.onComplete() // no receiving thread so explicitly complete subscriber
          close(consumer).recover {
            case th => logger.warn("An error occurred during closing consumer!", th)
          }
          logger.debug("Cancelled subscription to {}", destinationFactory)
        }

      @tailrec
      private def receiveMessage(): Unit = {
        receive(consumer).map {
          case Some(msg) =>
            logger.debug("Received {}", msg)
            subscriber.onNext(msg)
          case None =>
            logger.debug("Consumer possibly has been closed, completing subscriber")
            subscriber.onComplete()
        } recover {
          case th =>
            cancel()
            subscriber.onError(th)
        }
        if (requested.decrementAndGet() > 0 && !cancelled.get()) receiveMessage()
      }

      override def toString: String = "JmsSubscription(%s,%s)".format(connection, destinationFactory)
    }
  }
}
