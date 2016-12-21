package ru.reajames

import Jms._
import javax.jms._

import org.reactivestreams._

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.annotation.tailrec

/**
  * Represents a publisher in terms of reactive streams.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 21.12.16 0:14.
  */
class JmsPublisher(connectionFactory: ConnectionFactory, destinationFactory: DestinationFactory)
                  (implicit executionContext: ExecutionContext) extends Publisher[Message] {

  def subscribe(subscriber: Subscriber[_ >: Message]): Unit = {
    val subscription = for {
      c <- connection(connectionFactory)
      _ <- start(c)
      s <- session(c)
      d <- destination(s, destinationFactory)
      consumer <- consumer(s, d)
    } yield new Subscription {
      val cancelled = new AtomicBoolean(false)
      val requested = new AtomicLong(0)

      def cancel(): Unit = {
        cancelled.set(true)
        close(consumer).flatMap(_ => close(c))
      }

      @tailrec
      def receiveMessage(): Unit =
        if (!cancelled.get()) {
          receive(consumer) match {
            case Success(Some(msg)) => subscriber.onNext(msg)
            case Success(None) => subscriber.onComplete() // consumer and connection already have been closed
            case Failure(th) =>
              cancel()
              subscriber.onError(th)
          }
          if (requested.decrementAndGet() > 0) receiveMessage()
        }

      def request(n: Long): Unit =
        if (n <= 0)
          throw new IllegalArgumentException("Requested items should be greater then 0!")
        else if (requested.getAndAdd(n) == 0)
          executionContext.execute(() => receiveMessage())
    }

    subscription match {
      case Success(s) => subscriber.onSubscribe(s)
      case Failure(th) => subscriber.onError(th)
    }
  }
}
