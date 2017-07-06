package ru.reajames

import Jms._
import javax.jms._
import scala.util.Try
import org.reactivestreams._
import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions
import java.util.concurrent.CountDownLatch

/**
  * Provides helpful utilities for testing purposes.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 2:03.
  */
trait JmsUtilities {
  /**
    * Creates a connection with the specified connection factory.
    * @param connectionFactory specifies a factory for connection
    * @param credentials specifies user name and password
    * @param clientId specifies client identifier to set on created connection
    * @return created connection or failure
    */
  def connection(connectionFactory: ConnectionFactory,
                 credentials: Option[(String, String)] = None, clientId: Option[String] = None): Try[Connection] = Try {
    credentials
      .map {
        case (user, passwd) => connectionFactory.createConnection(user, passwd)
      }.getOrElse(connectionFactory.createConnection())
  }.map { c =>
    clientId.foreach(c.setClientID)
    c
  }

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
      p <- producer(s)
    } {
      messages.foreach(m => send(p, messageFactory(s, m), d))
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
      private val clientSubscribed = new CountDownLatch(1)
      private val subscribedToReceiver = new CountDownLatch(1)

      private var jmsSubscription: Subscription = _

      private val fakeSubscriber = new Subscriber[Message] {
        def onError(t: Throwable): Unit = error(t)

        def onSubscribe(s: Subscription): Unit = {
          jmsSubscription = s
          subscribedToReceiver.countDown()
          clientSubscribed.await()
        }

        def onComplete(): Unit = complete()

        def onNext(t: Message): Unit = next(t)
      }

      publisher.subscribe(fakeSubscriber)

      def subscribe(s: Subscriber[_ >: Message]): Unit = {
        real = s
        subscribedToReceiver.await()
        s.onSubscribe(new Subscription {
          def cancel(): Unit = {
            whenCancelled()
            real = null
            jmsSubscription.cancel()
          }
          def request(n: Long): Unit = jmsSubscription.request(n)
        })
        clientSubscribed.countDown()
      }

      def cancel(): Unit = complete()

      def next(msg: Message): Unit = {
        real.onNext(msg)
        counter -= 1
        if (counter <= 0) complete()
      }

      def error(th: Throwable): Unit = real.onError(th)

      def complete(): Unit = {
        jmsSubscription.cancel()
        real.onComplete()
        allPublished.trySuccess(true)
      }

      def completed: Future[Boolean] = allPublished.future
  }
}
