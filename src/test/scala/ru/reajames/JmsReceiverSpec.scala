package ru.reajames

import org.scalatest._
import scala.concurrent.Await
import javax.jms.{Message, TextMessage}
import scala.concurrent.duration.Duration
import java.util.concurrent.atomic.AtomicBoolean
import org.reactivestreams.{Subscriber, Subscription}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Specification on JmsPublisher.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 21.12.16 1:41.
  */
class JmsReceiverSpec extends FlatSpec with Matchers with JmsUtilities with FfmqConnectionFactoryAware {
  val connectionHolder = new ConnectionHolder(connectionFactory)

  "JmsReceiver" should "raise an exception in case whether subscriber is not specified" in {
    val queue = Queue("queue-5")
    val pub = new JmsReceiver(connectionHolder, queue)
    intercept[NullPointerException](pub.subscribe(null))
  }

  it should "not allow constructing an instance without required parameters" in {
    intercept[IllegalArgumentException] {
      new JmsReceiver(null, null)
    }
    intercept[IllegalArgumentException] {
      new JmsReceiver(connectionHolder, null)
    }
  }

  it should "publish messages arrived to a JMS queue" in {
    val queue = Queue("queue-6")
    val pub = stopper(new JmsReceiver(connectionHolder, queue), 500)

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
    val pub = new JmsReceiver(connectionHolder, queue)

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
    val subscribed = new CountDownLatch(1)
    val received = new CountDownLatch(100)

    val topic = Topic("topic-8")
    val pub = new JmsReceiver(connectionHolder, topic)

    @volatile var counter = 0
    pub.subscribe(TestSubscriber(
      subscribe = subscription => subscribed.countDown(),
      request = Some(100),
      next = (s, msg) => {
        counter += 1
        received.countDown()
      }
    ))

    subscribed.await(1, TimeUnit.SECONDS)
    sendMessages((1 to 200).map(_.toString), string2textMessage, topic)

    received.await(5, TimeUnit.SECONDS)

    counter should equal(100)
  }

  it should "publish messages from a topic to different subscribers" in {
    var res1, res2 = List.empty[String]
    val topic = Topic("topic-9")
    val consumingLatch = new CountDownLatch(200)
    val subscribed = new CountDownLatch(2)

    val pub = new JmsReceiver(connectionHolder, topic)
    pub.subscribe(
      TestSubscriber(
        subscribe = subsciption => subscribed.countDown(),
        request = Some(100),
        next = (s, msg) => {
          res1 ::= msg.asInstanceOf[TextMessage].getText
          consumingLatch.countDown()
        }
      ))
    pub.subscribe(
      TestSubscriber(
        subscribe = subscription => subscribed.countDown(),
        request = Some(100),
        next = (s, msg) => {
          res2 ::= msg.asInstanceOf[TextMessage].getText
          consumingLatch.countDown()
        }
    ))
    subscribed.await(500, TimeUnit.MILLISECONDS) // it needs to asynchronously subscribe for a topic

    val msgs = (1 to 100).map(_.toString)
    sendMessages(msgs, string2textMessage, topic)
    consumingLatch.await(3, TimeUnit.SECONDS)

    res1 should equal(msgs.reverse)
    res2 should equal(msgs.reverse)
  }

  it should "supply failed subscription" in {
    val failedPublisher = new JmsReceiver(new ConnectionHolder(failingConnectionFactory), TemporaryQueue())

    failedPublisher.subscribe(new Subscriber[Message] {
      def onError(th: Throwable): Unit = failedPublisher.logger.debug("Generated exception!", th)
      def onComplete(): Unit = ???
      def onNext(message: Message): Unit = ???
      def onSubscribe(s: Subscription): Unit = {
        s.request(1)
        s.cancel()
        assert(s === failedPublisher.FailedSubscription)
      }
    })
  }
}
