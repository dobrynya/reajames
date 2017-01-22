package ru.reajames

import akka.stream.scaladsl._
import akka.actor.ActorSystem
import scala.concurrent.Future
import akka.stream.ActorMaterializer
import javax.jms.{Message, TextMessage}
import org.scalatest._, time._, concurrent._

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

    whenReady(received) {
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
    val sendReplyToHeader: DestinationAwareMessageFactory[String] =
      (session, elem) =>
        (mutate(session.createTextMessage(elem))(_.setJMSReplyTo(Queue(elem)(session))), Queue("in")(session))

    val sender = new JmsSender[String](connectionHolder, sendReplyToHeader)
    Source(messagesToSend).runWith(Sink.fromSubscriber(sender))

    whenReady(messagesReceivedByClients, timeout(Span(15, Seconds))) {
      _ == messagesToSend
    }
  }

  "Jms components" should "create a channel through a queue" in {
    val messagesToSend = List("message 1", "message 2", "message 3")

    val serverIn = Queue("akka-q-1")
    val clientIn = Queue("akka-q-2")

    val result = Source.fromPublisher(new JmsReceiver(connectionHolder, clientIn))
      .collect(extractText)
      .take(messagesToSend.size)
      .runWith(Sink.seq).map(_.toList)

    def enrichReplyTo[T](replyTo: DestinationFactory)
                        (messageFactory: DestinationAwareMessageFactory[T]): DestinationAwareMessageFactory[T] =
      (session, element) =>
        messageFactory(session, element) match {
          case (msg, destination) => (mutate(msg)(_.setJMSReplyTo(replyTo(session))), destination)
        }

    Source.fromPublisher(new JmsReceiver(connectionHolder, serverIn)).collect {
      case msg: TextMessage => (msg.getText, msg.getJMSReplyTo)
    }.take(messagesToSend.size).runWith(Sink.fromSubscriber(new JmsSender(connectionHolder, replyTo(string2textMessage))))

    val clientRequests = new JmsSender[String](connectionHolder,
      enrichReplyTo(clientIn)(permanentDestination(serverIn)(string2textMessage)))
    Source(messagesToSend).runWith(Sink.fromSubscriber(clientRequests))

    whenReady(result, timeout(Span(15, Seconds))) {
      _ == messagesToSend
    }
  }

  def extractText: PartialFunction[Message, String] = {
    case msg: TextMessage => msg.getText
  }

  override protected def afterAll(): Unit = system.terminate()
}
