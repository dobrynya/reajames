package ru.reajames

import Jms._
import org.reactivestreams._
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import javax.jms.{MessageProducer, Session}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.{ConcurrentLinkedQueue, CopyOnWriteArraySet}

/**
  * Represents a processor in terms of reactive streams. It provides ability to send messages arrived from an upstream
  * to a JMS destination. Also it can notify subscribers in case of completed/failed upstream or a failure during delivering
  * messages to a JMS broker.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 3:49.
  * @param connectionHolder contains connection to create JMS related components
  * @param messageFactory creates JMS messages from data elements and provides destination for messages
  * @param executionContext executes sending messages
  * @tparam T data element type to be sent
  */
class JmsSender[T](connectionHolder: ConnectionHolder,
                   messageFactory: DestinationAwareMessageFactory[T])
                  (implicit executionContext: ExecutionContext) extends Processor[T, Nothing] with Logging {
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
           messageFactory: MessageFactory[T])(implicit executionContext: ExecutionContext) =
    this(connectionHolder, permanentDestination(destination)(messageFactory))

  private[reajames] var state: Subscriber[T] = Unsubscribed

  private[reajames] val subscribers = new CopyOnWriteArraySet[CompleteSubscription]()

  def subscribe(subscriber: Subscriber[_]): Unit = {
    val subscription = new CompleteSubscription(Some(subscriber))
    subscriber.onSubscribe(subscription)
    subscribers.add(subscription)
  }

  private[reajames] def notifySubscribers(cause: Option[Throwable]) =
    subscribers.forEach(_.completeWith(cause))

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

  private[reajames] object Unsubscribed extends Subscriber[T] {
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
          notifySubscribers(Some(th))
          state = Unsubscribed
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
    }

    def onNext(element: T): Unit = for (s <- subscriber) s.onNext(element)

    def onError(th: Throwable): Unit = for (s <- subscriber) s.onError(th)

    def onComplete(): Unit = for (s <- subscriber) s.onComplete()

    def onSubscribe(s: Subscription): Unit = s.cancel()
  }

  case class Subscribed(session: Session, producer: MessageProducer, subscription: Subscription)
    extends Subscriber[T] with Runnable {

    sealed trait Signal
    case class OnNext(element: T) extends Signal
    case class OnError(th: Throwable) extends Signal
    case object OnComplete extends Signal

    private[reajames] val sending = new AtomicBoolean(false)
    private[reajames] val queue = new ConcurrentLinkedQueue[Signal]()

    private[reajames] def unsubscribe(cause: Option[Throwable]): Unit = {
      subscription.cancel()
      state = Unsubscribed
      notifySubscribers(cause)
      close(producer) recover log("An error occurred during closing producer!")
      close(session) recover log("An error occurred during closing session!")
    }

    private[reajames] def doRequest: Unit = subscription.request(1)

    @tailrec
    final def run: Unit = {
      @tailrec
      def pollWhileNotEmpty: Unit =
        queue.poll() match {
          case OnNext(elem) =>
            Try(messageFactory(session, elem)) match {
              case Success((message, dest)) =>
                send(producer, message, dest) match {
                  case Success(msg) =>
                    logger.trace("Sent {}", msg)
                    doRequest
                    pollWhileNotEmpty
                  case Failure(th) =>
                    logger.warn(s"Could not send a message to $dest, closing producer!", th)
                    unsubscribe(Some(th))
                }
              case Failure(th) =>
                logger.warn(s"Could not send a message due to broken message factory, closing producer!", th)
                unsubscribe(Some(th))
            }
          case OnError(th) =>
            logger.warn(s"An error occurred in the upstream, closing producer $producer!", th)
            unsubscribe(Some(th))
          case `OnComplete` =>
            logger.debug("Upstream has been completed, closing {}", producer)
            unsubscribe(None)
          case null => // finish polling when the queue is empty
        }

      try pollWhileNotEmpty
      finally sending.set(false)
      if (!queue.isEmpty && sending.compareAndSet(false, true)) run
    }

    private[reajames] def scheduleRun: Unit =
      if (sending.compareAndSet(false, true)) executionContext.execute(this)

    @inline
    private def signal(signal: Signal) = if (queue.offer(signal)) scheduleRun

    def onNext(element: T): Unit = signal(OnNext(element))

    def onError(th: Throwable): Unit = signal(OnError(th))

    def onComplete(): Unit = signal(OnComplete)

    def onSubscribe(s: Subscription): Unit = s.cancel() // already subscribed
  }

  /**
    * Contains a subscriber to be notified in case of failure/completeness of the upstream or a failure of this JMS sender.
    * This subscription is not intended to signal onNext, so requests are just ignored.
    * @param subscriber subscriber to be notified
    */
  class CompleteSubscription(var subscriber: Option[Subscriber[_]]) extends Subscription {
    def request(n: Long): Unit = () // no onNext signal will be emitted
    def cancel(): Unit = completeWith(None)

    def completeWith(th: Option[Throwable]): Unit =
      for (s <- subscriber) {
        th.map(s.onError).getOrElse(s.onComplete())
        subscribers.remove(this)
        subscriber = None
      }
  }
}