package ru.reajames

import Jms._
import javax.jms._
import org.reactivestreams._
import scala.annotation.tailrec
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/**
  * Represents a publisher in terms of reactive streams.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 21.12.16 0:14.
  */
class JmsPublisher(connectionFactory: ConnectionFactory, destinationFactory: DestinationFactory)
                  (implicit executionContext: ExecutionContext) extends Publisher[Message] with Logging {

  def subscribe(subscriber: Subscriber[_ >: Message]): Unit = {
    if (subscriber == null)
      throw new NullPointerException("Subscriber should be specified!")

    val subscription = for {
      c <- connection(connectionFactory)
      _ <- start(c)
      s <- session(c)
      d <- destination(s, destinationFactory)
      consumer <- consumer(s, d)
    } yield new Subscription {
      val cancelled = new AtomicBoolean(false)
      val requested = new AtomicLong(0)

      def cancel(): Unit =
        if (cancelled.compareAndSet(false, true)) {
          if (requested.get() == 0) subscriber.onComplete() // no receiving thread so explicitly complete subscriber
          close(consumer).recover {
            case th => logger.warn("An error occurred during closing consumer!", th)
          }
          close(c).recover {
            case th => logger.error("An error occurred during closing connection!", th)
          }
          logger.debug("Cancelled subscription {}", this)
        }

      @tailrec
      def receiveMessage(): Unit = {
          receive(consumer).map {
            case Some(msg) => subscriber.onNext(msg)
            case None => subscriber.onComplete()
          } recover {
            case th =>
              cancel()
              subscriber.onError(th)
          }
          if (requested.decrementAndGet() > 0 && !cancelled.get()) receiveMessage()
        }

      def request(n: Long): Unit = {
        logger.debug("Requested {} from {}", n, this)
        if (n <= 0)
          throw new IllegalArgumentException("Requested items should be greater then 0!")
        else if (requested.getAndAdd(n) == 0)
          executionContext.execute(() => receiveMessage())
      }

      override def toString: String = "JmsSubscription(%s,%s)".format(connectionFactory, destinationFactory)
    }

    subscription match {
      case Success(s) =>
        logger.debug("{} has been subscribed to {} at {}", subscriber, destinationFactory, connectionFactory)
        subscriber.onSubscribe(s)
      case Failure(th) =>
        logger.warn("{} could not subscribe to {} at {}", subscriber, destinationFactory, connectionFactory)
        subscriber.onError(th)
    }
  }
}
