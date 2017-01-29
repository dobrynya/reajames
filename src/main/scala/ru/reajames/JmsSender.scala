package ru.reajames

import Jms._
import org.reactivestreams._
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
      (for {
        c <- connectionHolder.connection
        s <- session(c)
        p <- producer(s)
      } yield {
        logger.debug("Successfully created producer {}", p)
        state = Subscribed(s, p, subscription)
        subscription.request(1)
      }) recover {
        case th =>
          logger.error(s"Could not create producer using $connectionHolder!", th)
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

  case class Subscribed(session: Session, producer: MessageProducer, subscription: Subscription)
    extends Subscriber[T] {

    private[reajames] val sending = new AtomicBoolean(false)
    private[reajames] val queue = new ConcurrentLinkedQueue[(Int, Any)]()

    private def poll(): Unit = {
      while (!queue.isEmpty) {
        queue.poll() match {
          case (1, elem: T) =>
            val (message, destination) = messageFactory(session, elem)

            send(producer, message, destination) match {
              case Success(msg) =>
                logger.trace("Sent {}", msg)
                subscription.request(1)
              case Failure(th) =>
                logger.warn(s"Could not send a message to $destination, closing producer!", th)
                subscription.cancel()
                state = unsubscribed
                close(producer).recover {
                  case throwable => logger.warn("An error occurred during closing producer!", throwable)
                }
            }

          case (2, th: Throwable) =>
            logger.warn(s"An error occurred in the upstream, closing producer!", th)
            state = unsubscribed
            queue.clear()
            close(producer).recover {
              case throwable => logger.warn("An error occurred during closing producer!", throwable)
            }

          case (3, _) =>
            logger.debug("Upstream has been completed, closing {}", producer)
            state = unsubscribed
            queue.clear()
            close(producer).recover {
              case th => logger.warn("An error occurred when closing producer!", th)
            }
        }
      }

      sending.set(false)
    }

    private[reajames] def runIfNot: Unit =
      if (sending.compareAndSet(false, true)) Future (poll())

    def onNext(elem: T): Unit = {
      queue.offer(1 -> elem)
      runIfNot
    }

    def onError(th: Throwable): Unit = {
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