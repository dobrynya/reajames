package ru.reajames

import Jms._
import javax.jms._
import org.reactivestreams._
import scala.annotation.tailrec
import scala.util.{Failure, Success}
import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/**
  * Represents a publisher in terms of reactive streams.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 21.12.16 0:14.
  * @param connectionHolder contains connection to create JMS related components
  * @param destinationFactory specifies source of messages
  * @param executionContext executes receiving messages
  */
class JmsReceiver(connectionHolder: ConnectionHolder, destinationFactory: DestinationFactory, acknowledgeMode: Int = Session.AUTO_ACKNOWLEDGE)
                 (implicit executionContext: ExecutionContext) extends Publisher[Message] with Logging {
  require(connectionHolder != null, "Connection holder should be supplied!")
  require(destinationFactory != null, "Destination factory should be supplied!")

  def subscribe(subscriber: Subscriber[_ >: Message]): Unit = {
    if (subscriber == null)
      throw new NullPointerException("Subscriber should be specified!")

    Future {
      val subscription = for {
        c <- connectionHolder.connection
        s <- session(c, acknowledgeMode)
        d <- destination(s, destinationFactory)
        consumer <- consumer(s, d)
      }
        yield new JmsSubscription(c, s, consumer, subscriber)

      subscription match {
        case Success(jmsSubscription) =>
          logger.debug("Subscribed to {}", destinationFactory)
          subscriber.onSubscribe(jmsSubscription)
          jmsSubscription.startReceiving
        case Failure(th) =>
          logger.debug(s"Could not subscribe to $destinationFactory", th)
          subscriber.onSubscribe(FailedSubscription)
          subscriber.onError(th)
      }
    }
  }

  /**
    * Represents a failed subscription to pass it to a subscriber.
    */
  private[reajames] object FailedSubscription extends Subscription {
    def cancel(): Unit = ()
    def request(n: Long): Unit = ()
  }

  /**
    * Represents a connected subscription.
    * @param connection specifies connection of the subscription
    * @param session specifies current session
    * @param consumer specifies consumer
    * @param subscriber specifies the subscriber to be notified with messages
    */
  class JmsSubscription(connection: Connection, session: Session, consumer: MessageConsumer,
                        var subscriber: Subscriber[_ >: Message]) extends Subscription with Runnable {
    private val cancelled = new AtomicBoolean(false)
    private val requested = new AtomicLong(0)
    private val subscribed = new CountDownLatch(1)

    def request(n: Long): Unit = {
      logger.trace("Requested {} from {}", n, destinationFactory)
      if (n <= 0)
        cancel(Some(new IllegalArgumentException(s"Requested $n elements violating rule 3.9!")))
      else {
        val demand = Math.min(Long.MaxValue - requested.get(), n)
        if (requested.getAndAdd(demand) == 0)
          executionContext.execute(this)
      }
    }

    def cancel(): Unit = cancel(None)

    def startReceiving: Unit = subscribed.countDown()

    def run(): Unit = {
      subscribed.await()
      receiveMessage()
    }

    @inline
    def working = !cancelled.get()

    private[reajames] def cancel(cause: Option[Throwable]): Unit =
      if (cancelled.compareAndSet(false, true)) {
        close(consumer) recover log("An error occurred during closing consumer!")
        close(session) recover log("An error occurred during closing session!")

        cause.map { th =>
          logger.warn("Subscription has been cancelled due to an error!", th)
          subscriber.onError(th)
        } getOrElse {
          logger.debug("Cancelled subscription to {}", destinationFactory)
        }

        subscriber = null // drop subscriber
      }

    @tailrec
    private def receiveMessage(): Unit = {
      receive(consumer).map {
        _.foreach { msg =>
          logger.trace("Received {}", msg)
          subscriber.onNext(msg)
        }
      } recover {
        case th => cancel(Some(th))
      }

      if (requested.decrementAndGet() > 0 && working) receiveMessage()
    }

    override def toString: String = "JmsSubscription(%s,%s)".format(connection, destinationFactory)
  }
}