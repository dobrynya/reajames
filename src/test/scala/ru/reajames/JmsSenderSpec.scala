package ru.reajames

import Jms._
import scala.util._
import javax.jms.TextMessage
import scala.concurrent.Promise
import org.scalatest._
import time._
import concurrent._
import org.reactivestreams.Subscription
import org.apache.activemq.ActiveMQConnectionFactory
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Specification on JmsSender.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 16.01.17 15:38.
  */
class JmsSenderSpec extends FlatSpec with Matchers with JmsUtilities {
  private implicit val connectionFactory =
    new ActiveMQConnectionFactory("vm://test-broker?broker.persistent=false&broker.useJmx=false")

  "Unsubscribed" should "do nothing on all events except onSubscribe" in {
    val sender = new JmsSender[String](connectionFactory, Queue("queue-12"), _ => _ => ???)
    sender.onComplete()
    sender.onError(new Exception("Generated exception!"))
    sender.onNext("Test message")
  }

  "JmsSender" should "be able to connect the broker only once" in {
    val sender = new JmsSender[String](connectionFactory, Queue("queue-13"),
      session => elem => session.createTextMessage(elem))

    val requestedBySubscription = Promise[Boolean]()
    sender.onSubscribe(new Subscription {
      def cancel(): Unit = ???
      def request(n: Long): Unit = requestedBySubscription.complete(Success(true))
    })
    ScalaFutures.whenReady(requestedBySubscription.future, Timeout(Span(2, Seconds))) {
      _ should equal(true)
    }

    val cancelledBySubscription = Promise[Boolean]()
    sender.onSubscribe(new Subscription {
      def cancel(): Unit = cancelledBySubscription.complete(Success(true))
      def request(n: Long): Unit = ???
    })
    ScalaFutures.whenReady(cancelledBySubscription.future, Timeout(Span(2, Seconds))) {
      _ should equal(true)
    }
  }

  "JmsSender" should "be able to send messages" in {
    val queue = Queue("queue-14")
    val sender = new JmsSender[String](connectionFactory, queue, session => elem => session.createTextMessage(elem))

    val requested = Promise[Boolean]
    sender.onSubscribe(new Subscription {
      def cancel() = ???
      def request(n: Long) = requested.tryComplete(Success(true))
    })

    ScalaFutures.whenReady(requested.future, Timeout(Span(1, Second))) {
      _ should equal(true)
    }

    (1 to 50).map(_.toString).foreach(sender.onNext)

    Thread.sleep(1000)

    var counter = 0

    for {
      c <- connection(connectionFactory)
      _ <- start(c)
      s <- session(c)
      d <- destination(s, queue)
      cons <- consumer(s, d)
    } {
        (1 to 50).map(_ => receive(cons)).forall {
          case Success(Some(msg: TextMessage)) =>
            val old = counter
            counter = msg.getText.toInt
            counter - 1 == old
      }
      close(c)
    }
  }
}
