package ru.reajames

import akka.stream.scaladsl._
import scala.concurrent.Future
import akka.stream.ActorMaterializer
import akka._
import pattern.{after => afterDelay}
import actor.ActorSystem
import org.scalatest._
import time._
import concurrent._
import javax.jms.{Message, Session, TextMessage}
import akka.stream.ThrottleMode.Shaping
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Tests using JMS connectors in the Akka Stream infrastructure.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 2:44.
  */
class JmsConnectorsInAkkaStreamsTest extends FlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures
  with ActimeMQConnectionFactoryAware with JmsUtilities {
  behavior of "JMS connectors"

  implicit val system = ActorSystem("test")
  implicit val ec = system.dispatchers.lookup("akka.stream.default-blocking-io-dispatcher")
  implicit val mat = ActorMaterializer()

  val connectionHolder = new ConnectionHolder(connectionFactory)

  "JmsReceiver" should "receive messages and pass it to akka stream for processing" in {
    val queue = Queue("queue-10")
    val messagesToBeSent = (1 to 50).map(_.toString).toList

    val receiver = new JmsReceiver(connectionHolder, queue)
    val received = Source.fromPublisher(receiver).collect(extractText).take(25).runWith(Sink.seq)

    sendMessages(messagesToBeSent, string2textMessage, queue)

    whenReady(received, timeout(Span(5, Seconds))) {
      _.toList == messagesToBeSent.take(25)
    }
  }

  "Jms connectors" should "send and receive messages processing it within akka stream" in {
    val queue = Queue("queue-11")

    val messagesToSend = (1 to 100).map(_.toString).toList

    val received = Source.fromPublisher(new JmsReceiver(connectionHolder, queue)).collect {
      case msg: TextMessage => msg.getText
    }.take(messagesToSend.size).runWith(Sink.seq)

    val sender = new JmsSender[String](connectionHolder, permanentDestination(queue)(string2textMessage))
    Source(messagesToSend).runWith(Sink.fromSubscriber(sender))

    whenReady(received, timeout(Span(10, Seconds))) {
      _.toList == messagesToSend
    }
  }

  "JmsSender" should "allow to send to an appropriate JMS destination" in {
    val messagesToSend = List("queue-11-1", "queue-11-2", "queue-11-3")

    val received = Future.sequence(
      messagesToSend
        .map(q => new JmsReceiver(connectionHolder, Queue(q)))
        .map(Source.fromPublisher).map(_.collect(extractText).take(1).runWith(Sink.head))
    )

    // Using a message as a queue name to send message to
    val sendMessagesToDifferentQueues: DestinationAwareMessageFactory[String] =
      (session, elem) => (session.createTextMessage(elem), Queue(elem)(session))

    val sender = new JmsSender[String](connectionHolder, sendMessagesToDifferentQueues)
    Source(messagesToSend).runWith(Sink.fromSubscriber(sender))

    whenReady(received, timeout(Span(10, Seconds))) {
      _ == messagesToSend
    }
  }

  "Jms pipeline" should "respond to destination specified in the JMSReplyTo header" in {
    val messagesToSend = List("queue-11-4", "queue-11-5", "queue-11-6")

    val messagesReceivedByClients = Future.sequence(
      messagesToSend
        .map(q => new JmsReceiver(connectionHolder, Queue(q)))
        .map(Source.fromPublisher).map(_.collect(extractText).take(1).runWith(Sink.head))
    )

    // Main pipeline
    Source.fromPublisher(new JmsReceiver(connectionHolder, Queue("in"))).take(3)
      .collect {
        case msg: TextMessage => (msg.getText, msg.getJMSReplyTo) // store JMSReplyTo header with message body
      }.runWith(Sink.fromSubscriber(new JmsSender(connectionHolder, replyTo(string2textMessage))))

    // just enriches a newly created text message with JMSReplyTo
    val replyToElemName = (session: Session, elem: String) =>
      mutate(session.createTextMessage(elem))(_.setJMSReplyTo(Queue(elem)(session)))

    val sender = new JmsSender(connectionHolder, Queue("in"), replyToElemName)
    Source(messagesToSend).runWith(Sink.fromSubscriber(sender))

    whenReady(messagesReceivedByClients, timeout(Span(5, Seconds))) {
      _ == messagesToSend
    }
  }

  "Jms components" should "create a channel through a temporary queue" in {
    val messagesToSend = List("message 1", "message 2", "message 3")

    val serverIn = Queue("akka-q-1")
    val clientIn = TemporaryQueue()

    val result = Source.fromPublisher(new JmsReceiver(connectionHolder, clientIn))
      .collect(extractText)
      .take(messagesToSend.size)
      .runWith(Sink.seq).map(_.toList)

    Source.fromPublisher(new JmsReceiver(connectionHolder, serverIn)).collect {
      case msg: TextMessage => (msg.getText, msg.getJMSReplyTo)
    }.take(messagesToSend.size).runWith(Sink.fromSubscriber(new JmsSender(connectionHolder, replyTo(string2textMessage))))

    val clientRequests = new JmsSender[String](connectionHolder, serverIn, enrichReplyTo(clientIn)(string2textMessage))
    Source(messagesToSend).runWith(Sink.fromSubscriber(clientRequests))

    whenReady(result, timeout(Span(5, Seconds))) { _ == messagesToSend }
  }

  "JmsSender" should "allow to detect a failure to recreate a stream" in {
    val queue = TemporaryQueue()
    val messages = Iterator("1", "2", "3")

    def createSendingStream(connectionRescheduler: => Future[Boolean]): Future[Done] = {
      val mf: MessageFactory[String] =
        (session, element) =>
          if (element != "1") session.createTextMessage(element)
          else throw new IllegalArgumentException(s"Could not send $element due to the test case!")

      val sender = new JmsSender(connectionHolder, queue, mf)
      Source.fromIterator(() => messages)
        .throttle(1, 50 millis, 1, Shaping)
        .via(Flow.fromProcessor(() => sender))
        .runWith(Sink.ignore)
        .recoverWith {
          case th =>
            connectionRescheduler.flatMap(_ => createSendingStream(connectionRescheduler))
        }
    }

    var toBeTaken = 2

    def createReceivingStream(rescheduler: => Future[_]): Future[Seq[String]] = {
      Source.fromPublisher(new JmsReceiver(connectionHolder, queue))
        .take(toBeTaken)
        .collect(extractText)
        .filter(s => if (s != "2") true else throw new IllegalArgumentException(s"Could not process $s due to the test case!"))
        .runWith(Sink.seq)
        .recoverWith {
          case th =>
            th.printStackTrace()
            toBeTaken -= 1
            rescheduler.flatMap(_ => createReceivingStream(rescheduler))
        }
    }

    val sent =
      createSendingStream(afterDelay(100 millis, system.scheduler)(Future.successful(true)))
    val received =
      createReceivingStream(afterDelay(100 millis, system.scheduler)(Future.successful(true)))

    whenReady(sent, timeout(Span(1, Second)))(_ should equal(Done))
    whenReady(received, timeout(Span(1, Second)))(_ == Seq("3"))
  }

  def extractText: PartialFunction[Message, String] = {
    case msg: TextMessage => msg.getText
  }

  override protected def afterAll(): Unit = system.terminate()
}
