package ru.reajames

import Jms._
import scala.util._
import javax.jms.TextMessage
import scala.concurrent.Promise
import org.reactivestreams.Subscription
import org.scalatest._, concurrent._, time._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Specification on JmsSender.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 16.01.17 15:38.
  */
class JmsSenderSpec extends FlatSpec with Matchers with ScalaFutures with TimeLimits
  with ActimeMQConnectionFactoryAware with JmsUtilities {

  val connectionHolder = new ConnectionHolder(connectionFactory)

  "Unsubscribed" should "do nothing on all events except onSubscribe" in {
    val sender = new JmsSender[String](connectionHolder, permanentDestination(Queue("queue-12"))(string2textMessage))
    sender.onComplete()
    sender.onError(new Exception("Generated exception!"))
    sender.onNext("Test message")
  }

  "JmsSender" should "be able to connect the broker only once" in {
    val sender = new JmsSender[String](connectionHolder, permanentDestination(Queue("queue-13"))(string2textMessage))

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

    val sender = new JmsSender[String](connectionHolder, permanentDestination(queue)(string2textMessage))
    QueuePublisher(List("message to send")).subscribe(sender)

    failAfter(Span(500, Millis)) {
      val received = for {
        c <- connectionHolder.connection
        _ <- start(c)
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
        close(c)
        res
      }

      received should matchPattern {
        case Success(seq: Seq[Try[Option[TextMessage]]]) if seq.forall(_.isSuccess) =>
      }
      val messages = received.get.map(_.map(_.map(_.asInstanceOf[TextMessage]).get).get)
      messages.forall(m => m.getText == m.getJMSCorrelationID) should equal(true)
    }
  }

  override implicit def patienceConfig = PatienceConfig(Span(500, Millis))
}
