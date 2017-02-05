package ru.reajames

import Jms._
import org.reactivestreams._
import scala.annotation.tailrec
import scala.util.{Failure, Success}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.{ExecutionContext, Future}
import javax.jms.{Message, MessageProducer, Session}

/**
  * Represents a subscriber in terms of reactive streams. It provides ability to connect a JMS destination and
  * listen to messages.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 3:49.
  * @param connectionHolder contains connection to create JMS related components
  * @param messageFactory creates JMS messages from data elements and provides destination for messages
  * @param executionContext executes sending messages
  */
class JmsSender[T](connectionHolder: ConnectionHolder,
                   messageFactory: DestinationAwareMessageFactory[T])
                  (implicit executionContext: ExecutionContext) extends Subscriber[T] with Logging {
  require(connectionHolder != null, "Connection holder should be supplied!")
  require(messageFactory != null, "Destination aware message factory should be supplied!")

  /**
    * Creates a JMS sender to send messages to a permanent destination.
    * @param connectionHolder connection holder
    * @param destination destination for messages
    * @param messageFactory message factory
    * @param executionContext executes sending messages
    * @return created sender
    */
  def this(connectionHolder: ConnectionHolder, destination: DestinationFactory,
           messageFactory: (Session, T) => Message)(implicit executionContext: ExecutionContext) =
    this(connectionHolder, permanentDestination(destination)(messageFactory))

  private[reajames] var state: Subscriber[T] = new Unsubscribed

  def onSubscribe(subscription: Subscription): Unit =
    if (subscription != null) state.onSubscribe(subscription)
    else throw new NullPointerException("Subscription should be specified!")

  def onNext(element: T): Unit = {
    if (element == null) throw new NullPointerException("Element should be specified!")
    state.onNext(element)
  }

  def onComplete(): Unit = state.onComplete()

  def onError(th: Throwable): Unit = {
    if (th == null) throw new NullPointerException("Throwable should be specified!")
    state.onError(th)
  }

  class Unsubscribed extends Subscriber[T] {
    private[reajames] def tryToConnect(subscription: Subscription) =
      for {
        c <- connectionHolder.connection
        s <- session(c)
        p <- producer(s)
      } yield {
        logger.debug("Successfully created producer {}", p)
        Subscribed(s, p, subscription)
      }

    def onSubscribe(subscription: Subscription): Unit =
      state = new Subscribing(Future(tryToConnect(subscription)).flatMap(Future.fromTry).recoverWith {
        case th =>
          logger.debug("Could not establish connection!", th)
          subscription.cancel()
          state = new Unsubscribed
          Future.failed(th)
      })

    def onError(th: Throwable): Unit =
      logger.warn("JmsSender is unsubscribed but onError has been received!", th)

    def onComplete(): Unit =
      logger.warn("JmsSender is unsubscribed but onComplete has been received!")

    def onNext(element: T): Unit =
      logger.warn("JmsSender is unsubscribed but onNext({}) has been received!", element)
  }

  class Subscribing(subscriber: Future[Subscribed]) extends Subscriber[T] {

    for (subscribed <- subscriber) {
      state = subscribed
      subscribed.doRequest
      logger.debug("Successfully created a message producer")
    }

    def onNext(element: T): Unit = for (s <- subscriber) s.onNext(element)

    def onError(th: Throwable): Unit = for (s <- subscriber) s.onError(th)

    def onComplete(): Unit = for (s <- subscriber) s.onComplete()

    def onSubscribe(s: Subscription): Unit = s.cancel()
  }

  case class Subscribed(session: Session, producer: MessageProducer, subscription: Subscription)
    extends Subscriber[T] {

    private[reajames] val sending = new AtomicBoolean(false)
    private[reajames] val queue = new ConcurrentLinkedQueue[(Int, Any)]()

    private[reajames] def unsubscribe: Unit = {
      subscription.cancel()
      state = new Unsubscribed
      close(producer).recover {
        case throwable => logger.warn("An error occurred during closing producer!", throwable)
      }
      close(session).recover {
        case throwable => logger.warn("An error occurred during closing session!", throwable)
      }
    }

    private[reajames] def doRequest: Unit = subscription.request(1)

    private def poll(): Unit = {
      @tailrec
      def pollIfNotEmpty: Unit = {
        queue.poll() match {
          case (1, elem: T) =>
            val (message, destination) = messageFactory(session, elem)

            send(producer, message, destination) match {
              case Success(msg) =>
                logger.trace("Sent {}", msg)
                doRequest
              case Failure(th) =>
                logger.warn(s"Could not send a message to $destination, closing producer!", th)
                unsubscribe
            }

          case (2, th: Throwable) =>
            logger.warn(s"An error occurred in the upstream, closing producer!", th)
            unsubscribe
          case (3, _) =>
            logger.debug("Upstream has been completed, closing {}", producer)
            unsubscribe
        }

        if (sending.compareAndSet(false, true))
        if (!queue.isEmpty) pollIfNotEmpty
      }

      pollIfNotEmpty
      sending.set(false)
      if (!queue.isEmpty) runIfNot
    }

    private[reajames] def runIfNot: Unit =
      if (sending.compareAndSet(false, true)) Future(poll())

    def onNext(element: T): Unit = {
      if (element == null) throw new NullPointerException("Element should be specified!")
      queue.offer(1 -> element)
      runIfNot
    }

    def onError(th: Throwable): Unit = {
      if (th == null) throw new NullPointerException("Throwable should be specified!")
      queue.offer(2 -> th)
      runIfNot
    }

    def onComplete(): Unit = Future {
      queue.offer(3 -> null)
      runIfNot
    }

    def onSubscribe(s: Subscription): Unit = s.cancel() // already subscribed
  }
}