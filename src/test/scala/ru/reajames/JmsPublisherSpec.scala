package ru.reajames

import java.util.concurrent.Executors

import Jms._
import org.scalatest._
import javax.jms.{Message, TextMessage}

import org.apache.activemq.ActiveMQConnectionFactory
import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.ExecutionContext

/**
  * Specification on JmsPublisher.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 21.12.16 1:41.
  */
class JmsPublisherSpec extends FlatSpec with Matchers {
  val connectionFactory = new ActiveMQConnectionFactory("vm://test-broker?broker.persistent=false&broker.useJmx=false")

  val queue = Queue("queue")

  "JmsPublisher" should "publish messages arrived to a JMS queue" in {
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val pub = new JmsPublisher(connectionFactory, queue)

    var received = List.empty[String]
    pub.subscribe(new Subscriber[Message] {
      var subscription = Option.empty[Subscription]

      def onError(th: Throwable): Unit = th.printStackTrace()

      def onSubscribe(s: Subscription): Unit = {
        subscription = Some(s)
        s.request(Long.MaxValue)
      }

      def onComplete(): Unit = subscription = None

      def onNext(msg: Message): Unit = msg match {
        case text: TextMessage =>
          received ::= text.getText
          if (received.head == "500") subscription.foreach(_.cancel())
      }
    })

    val expected = (1 to 500).map(_.toString).toList

    for {
      c <- connection(connectionFactory)
      s <- session(c)
      d <- destination(s, queue)
      p <- producer(s, d)
    } {
      expected.foreach(m => send(p, string2textMessage(s)(m)))
      close(c)
    }

    received.reverse should equal(expected)
  }
}
