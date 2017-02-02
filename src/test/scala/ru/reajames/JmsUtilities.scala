package ru.reajames

import java.util.concurrent.CountDownLatch

import Jms._
import javax.jms._

import org.reactivestreams._

import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions

/**
  * Provides helpful utilities for testing purposes.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 2:03.
  */
trait JmsUtilities {
  /**
    * Sends messages to the specified destination and closes connection.
    * @param messages messages to be sent
    * @param messageFactory a factory to convert messages
    * @param destinationFactory a destination
    * @param connectionFactory connection factory
    * @tparam T data element type
    */
  def sendMessages[T](messages: Traversable[T], messageFactory: (Session, T) => Message,
                      destinationFactory: DestinationFactory)
                     (implicit connectionFactory: ConnectionFactory): Unit = {
    for {
      c <- connection(connectionFactory)
      s <- session(c)
      d <- destination(s, destinationFactory)
      p <- producer(s, d)
    } {
      messages.foreach(m => send(p, messageFactory(s, m)))
      close(c)
    }
  }

  /**
    * Wraps a real JMS publisher with ability to stop after requested items have been received.
    * @param publisher a publisher to wrap
    * @param messagesToReceive amount of messages to receive, infinite by default
    * @return newly created spy publisher
    */
  def stopper(publisher: JmsReceiver, messagesToReceive: Long = Long.MaxValue,
              whenCancelled: () => Unit = () => ()) =

    new Publisher[Message] {
      private var counter = messagesToReceive
      private val allPublished: Promise[Boolean] = Promise()

      private var real: Subscriber[_ >: Message] = _
      private val subscribed = new CountDownLatch(1)
      private val fake = TestSubscriber(
        subscribe = subscription => subscribed.countDown(),
        next = (s, m) => next(m),
        complete = () => complete(),
        error = (th) => error(th)
      )
      publisher.subscribe(fake)

      def subscribe(s: Subscriber[_ >: Message]): Unit = {
        real = s
        subscribed.await()
        s.onSubscribe(new Subscription {
          def cancel(): Unit = {
            whenCancelled()
            real = null
            fake.subscription.cancel()
          }
          def request(n: Long): Unit = fake.subscription.request(n)
        })
      }

      def cancel(): Unit = complete()

      def request(n: Long): Unit = fake.subscription.request(n)

      def next(msg: Message): Unit = {
        real.onNext(msg)
        counter -= 1
        if (counter <= 0) complete()
      }

      def error(th: Throwable): Unit = real.onError(th)

      def complete(): Unit = {
        fake.subscription.cancel()
        real.onComplete()
        allPublished.trySuccess(true)
      }

      def completed: Future[Boolean] = allPublished.future
  }
}
