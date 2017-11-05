package ru.reajames

import Jms._
import scala.util._
import javax.jms.TextMessage
import scala.concurrent.Promise
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatest._, concurrent._, time._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Specification on JmsSender.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 16.01.17 15:38.
  */
class JmsSenderSpec extends FlatSpec with Matchers with ScalaFutures with TimeLimits
  with FfmqConnectionFactoryAware with JmsUtilities {

  val connectionHolder = new ConnectionHolder(connectionFactory)

  "Unsubscribed" should "do nothing on all events except onSubscribe" in {
    val sender = new JmsSender[String](connectionHolder, Queue("queue-12"), string2textMessage)
    sender.onComplete()
    sender.onError(new Exception("Generated exception!"))
    sender.onNext("Test message")
  }

  "Unsubscribed" should "throw an exception when no subscription is supplied" in {
    intercept[NullPointerException] {
      new JmsSender[String](connectionHolder, Queue("some-queue"), string2textMessage)
        .onSubscribe(null)
    }
  }

  "JmsSender" should "not allow constructing an instance without required parameters" in {
    intercept[IllegalArgumentException] {
      new JmsSender[String](null, null)
    }
    intercept[IllegalArgumentException] {
      new JmsSender[String](connectionHolder, null)
    }
  }

  "JmsSender" should "be able to connect the broker only once" in {
    val sender = new JmsSender[String](connectionHolder, Queue("queue-13"), string2textMessage)

    val requestedBySubscription = Promise[Boolean]()
    sender.onSubscribe(new Subscription {
      def cancel(): Unit = ???
      def request(n: Long): Unit = requestedBySubscription.complete(Success(true))
    })
    requestedBySubscription.future.futureValue should equal(true)

    val cancelledBySubscription = Promise[Boolean]()
    sender.onSubscribe(new Subscription {
      def cancel(): Unit = cancelledBySubscription.complete(Success(true))
      def request(n: Long): Unit = ???
    })
    cancelledBySubscription.future.futureValue should equal(true)
  }

  "JmsSender" should "be able to send messages to a permanently specified destination" in {
    val queue = Queue("queue-14")

    val sender = new JmsSender[String](connectionHolder, queue, string2textMessage)
    QueuePublisher(List("message to send")).subscribe(sender)

    failAfter(Span(1000, Millis)) {
      val received = for {
        c <- connectionHolder.connection
        s <- session(c)
        d <- destination(s, queue)
        cons <- consumer(s, d)
        received <- receive(cons)
      } yield received

      received should matchPattern {
        case Success(Some(msg: TextMessage)) if msg.getText == "message to send" =>
      }
    }
  }

  "JmsSender" should "send messages with specifying message headers" in {
    val in = Queue("queue-15")
    val out = Queue("queue-16")

    // enriches a newly created message with JMSCorrelationID header
    def corrId: DestinationAwareMessageFactory[String] = (session, elem) =>
      (mutate(session.createTextMessage(elem))(_.setJMSCorrelationID(elem)), out(session))

    val sender = new JmsSender[String](connectionHolder, corrId)

    val messagesToSend = List("m1", "m2", "m3")

    QueuePublisher(messagesToSend).subscribe(sender)

    failAfter(Span(1, Second)) {
      val received = for {
        c <- connectionHolder.connection
        _ <- start(c)
        s <- session(c)
        d <- destination(s, out)
        cons <- consumer(s, d)
      } yield {
        val res = (1 to 3).map(_ => receive(cons))
        res
      }

      received should matchPattern {
        case Success(seq: Seq[Try[Option[TextMessage]]]) if seq.forall(_.isSuccess) =>
      }
      val messages = received.get.map(_.map(_.map(_.asInstanceOf[TextMessage]).get).get)
      messages.forall(m => m.getText == m.getJMSCorrelationID) should equal(true)
    }
  }

  "JmsSender" should "allow to subscribe on completed/failed signal" in {
    val sender = new JmsSender[String](connectionHolder, TemporaryQueue(), string2textMessage)

    sender.subscribe(new Subscriber[Nothing] {
      def onSubscribe(s: Subscription): Unit = ()
      def onError(t: Throwable) = throw new Exception("No signal as sender is not subscribed!")
      def onComplete() = throw new Exception("No signal as sender is not subscribed!")
      def onNext(t: Nothing) = throw new Exception("No signal as sender is not subscribed!")
    })
  }

  "JmsSender" should "notify subscribers on completed stream" in {
    val sender = new JmsSender[String](connectionHolder, TemporaryQueue(), string2textMessage)

    val completed = Promise[Boolean]

    sender.subscribe(new Subscriber[Nothing] {
      def onSubscribe(s: Subscription): Unit = ()
      def onError(th: Throwable): Unit = completed.failure(th)
      def onComplete(): Unit = completed.success(true)
      def onNext(t: Nothing): Unit = completed.failure(new IllegalStateException("onNext signal should not be emitted!"))
    })

    QueuePublisher(List("1")).subscribe(sender)
    whenReady(completed.future)(_ should equal(true))
    sender.subscribers shouldBe empty
  }

  "JmsSender" should "notify subscribers on failed stream" in {
    val sender = new JmsSender[String](connectionHolder, Queue(null), string2textMessage)

    val completed = Promise[Boolean]

    sender.subscribe(new Subscriber[Nothing] {
      def onSubscribe(s: Subscription): Unit = ()
      def onError(th: Throwable): Unit = completed.success(true)
      def onComplete(): Unit = completed.failure(new IllegalStateException("onComplete signal should not be emitted!"))
      def onNext(t: Nothing): Unit = completed.failure(new IllegalStateException("onNext signal should not be emitted!"))
    })

    QueuePublisher(List("1")).subscribe(sender)
    whenReady(completed.future)(_ should equal(true))
    sender.subscribers shouldBe empty
  }

  "JmsSender" should "notify subscribers on failed connection" in {
    val sender = new JmsSender[String](new ConnectionHolder(failingConnectionFactory), Queue(null), string2textMessage)

    val completed = Promise[Boolean]

    sender.subscribe(new Subscriber[Nothing] {
      def onSubscribe(s: Subscription): Unit = ()
      def onError(th: Throwable): Unit = {
        th.printStackTrace()
        completed.success(true)
      }
      def onComplete(): Unit = completed.failure(new IllegalStateException("onComplete signal should not be emitted!"))
      def onNext(t: Nothing): Unit = completed.failure(new IllegalStateException("onNext signal should not be emitted!"))
    })

    QueuePublisher(List("1")).subscribe(sender)
    whenReady(completed.future)(_ should equal(true))
    sender.subscribers shouldBe empty
  }

  override implicit def patienceConfig = PatienceConfig(Span(500, Millis))
}
