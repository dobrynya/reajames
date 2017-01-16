package ru.reajames

import org.scalatest._
import javax.jms.TextMessage
import scala.concurrent.Await
import scala.language.reflectiveCalls
import scala.concurrent.duration.Duration
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.activemq.ActiveMQConnectionFactory
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Specification on JmsPublisher.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 21.12.16 1:41.
  */
class JmsReceiverSpec extends FlatSpec with Matchers with JmsUtilities {
  private implicit val connectionFactory =
    new ActiveMQConnectionFactory("vm://test-broker?broker.persistent=false&broker.useJmx=false")

  "JmsReceiver" should "raise an exception in case whether subscriber is not specified" in {
    val queue = Queue("queue-5")
    val pub = new JmsReceiver(connectionFactory, queue)
    intercept[NullPointerException](pub.subscribe(null))
  }

  it should "publish messages arrived to a JMS queue" in {
    val queue = Queue("queue-6")
    val pub = stopper(new JmsReceiver(connectionFactory, queue), 500)

    var received = List.empty[String]
    pub.subscribe(TestSubscriber(
      request = Some(Long.MaxValue),
      next = { (s, msg) =>
        val text = msg.asInstanceOf[TextMessage].getText
        received ::= text
      }
    ))

    val expected = (1 to 500).map(_.toString).toList
    sendMessages(expected, string2textMessage, queue)

    Await.ready(pub.completed, Duration.Inf)
    received.reverse should equal(expected)
  }

  it should "not call subscriber concurrently" in {
    val queue = Queue("queue-7")
    val pub = new JmsReceiver(connectionFactory, queue)

    var checked = true
    val called = new AtomicBoolean(false)

    def checkCalled(): Unit = {
      checked = called.compareAndSet(false, true)
      Thread.sleep(100)
      checked = called.compareAndSet(true, false)
    }

    pub.subscribe(TestSubscriber(
      request = Some(1000),
      subscribe = _ => checkCalled(),
      error = _ => checkCalled(),
      complete = () => checkCalled(),
      next = (s, msg) => if (msg.asInstanceOf[TextMessage].getText == "1000") s.cancel()
    ))

    sendMessages((1 to 1000).map(_.toString), string2textMessage, queue)
    checked should equal(true)
  }

  it should "receive only requested amount of messages" in {
    val topic = Topic("topic-8")
    val pub = new JmsReceiver(connectionFactory, topic)

    @volatile var counter = 0
    pub.subscribe(TestSubscriber(request = Some(100), next = (s, msg) => counter += 1))

    sendMessages((1 to 200).map(_.toString), string2textMessage, topic)

    counter should equal(100)
  }

  it should "publish messages from a topic to different subscribers" in {
    var res1, res2 = List.empty[String]
    val topic = Topic("topic-9")
    val pub = new JmsReceiver(connectionFactory, topic)
    pub.subscribe(TestSubscriber(
      request = Some(100),
      next = (s, msg) => res1 ::= msg.asInstanceOf[TextMessage].getText
    ))
    pub.subscribe(TestSubscriber(
      request = Some(100),
      next = (s, msg) => res2 ::= msg.asInstanceOf[TextMessage].getText
    ))

    val msgs = (1 to 100).map(_.toString)
    sendMessages(msgs, string2textMessage, topic)

    Thread.sleep(1000) // TODO: Implement another waiting strategy!

    res1 should equal(msgs.reverse)
    res2 should equal(msgs.reverse)
  }
}
