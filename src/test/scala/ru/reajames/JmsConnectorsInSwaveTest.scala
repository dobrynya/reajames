package ru.reajames

import swave.core._
import java.util.concurrent.Executors
import org.scalatest._, concurrent._, time._
import scala.concurrent.{ExecutionContext, Future}
import javax.jms.{Message, TextMessage, Destination => JmsDestination}

/**
  * Tests using JMS connectors in the Swave infrastructure.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 2:44.
  */
class JmsConnectorsInSwaveTest extends FlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures
  with ActimeMQConnectionFactoryAware with JmsUtilities {
  behavior of "JMS connectors"

  implicit val env = StreamEnv()
  implicit val ctx = ExecutionContext.fromExecutor(Executors.newScheduledThreadPool(12))

  val connectionHolder = new ConnectionHolder(connectionFactory)

  "JmsReceiver" should "receive messages and pass it to stream for processing" in {
    val queue = Queue("queue-10")
    val messagesToBeSent = (1 to 50).map(_.toString).toList

    val receiver = new JmsReceiver(connectionHolder, queue)
    val received = Spout.fromPublisher(receiver).collect(extractText).take(25).drainToList(50)

    sendMessages(messagesToBeSent, string2textMessage, queue)

    whenReady(received, timeout(Span(500, Millis))) {
      _ == messagesToBeSent.take(25)
    }
  }

  "Jms connectors" should "send and receive messages processing it within akka stream" in {
    val queue = Queue("queue-11")

    val messagesToSend = (1 to 100).map(_.toString).toList

    val received = Spout.fromPublisher(new JmsReceiver(connectionHolder, queue)).collect {
      case msg: TextMessage => msg.getText
    }.take(messagesToSend.size).drainToList(100)

    val sender = new JmsSender[String](connectionHolder, permanentDestination(queue)(string2textMessage))
    Spout(messagesToSend).drainTo(Drain.fromSubscriber(sender))

    whenReady(received, timeout(Span(10, Seconds))) {
      _ == messagesToSend
    }
  }

  "JmsSender" should "allow to send to an appropriate JMS destination" in {
    val messagesToSend = List("queue-11-1", "queue-11-2", "queue-11-3")

    val received = Future.sequence(
      messagesToSend
        .map(q => new JmsReceiver(connectionHolder, Queue(q)))
        .map(Spout.fromPublisher).map(_.collect(extractText).take(1).drainToHead())
    )

    // Using a message as a queue name to send message to
    val sendMessagesToDifferentQueues: DestinationAwareMessageFactory[String] =
      (session, elem) => (session.createTextMessage(elem), Queue(elem)(session))

    val sender = new JmsSender[String](connectionHolder, sendMessagesToDifferentQueues)
    Spout(messagesToSend).drainTo(Drain.fromSubscriber(sender))

    whenReady(received, timeout(Span(10, Seconds))) {
      _ == messagesToSend
    }
  }

  "Jms pipeline" should "respond to destination specified in the JMSReplyTo header" in {
    val messagesToSend = List("queue-11-4", "queue-11-5", "queue-11-6")

    val messagesReceivedByClients = Future.sequence(
      messagesToSend
        .map(q => new JmsReceiver(connectionHolder, Queue(q)))
        .map(Spout.fromPublisher).map(_.collect(extractText).take(1).drainToHead())
    )

    val serverIn = Queue("queue-11-in")

    // Main pipeline
    Spout.fromPublisher(new JmsReceiver(connectionHolder, serverIn)).take(3).collect {
      case msg: TextMessage => (msg.getText, msg.getJMSReplyTo)
    }.drainTo(Drain.fromSubscriber(new JmsSender(connectionHolder, replyTo(string2textMessage))))

    // just enriches a newly created text message with JMSReplyTo
    val sendReplyToHeader: DestinationAwareMessageFactory[String] =
      (session, elem) =>
        (mutate(session.createTextMessage(elem))(_.setJMSReplyTo(Queue(elem)(session))), serverIn(session))

    val sender = new JmsSender[String](connectionHolder, sendReplyToHeader)
    Spout(messagesToSend).drainTo(Drain.fromSubscriber(sender))

    whenReady(messagesReceivedByClients, timeout(Span(10, Seconds))) {
      _ == messagesToSend
    }
  }

  "Jms components" should "create a channel through a temporary queue" in {
    val messagesToSend = List("message 1", "message 2", "message 3")

    val serverIn = Queue("swave-q-1")
    val clientIn = TemporaryQueue()

    val result = Spout.fromPublisher(new JmsReceiver(connectionHolder, clientIn))
      .collect(extractText)
      .take(messagesToSend.size)
      .drainToList(messagesToSend.size)

    Spout.fromPublisher(new JmsReceiver(connectionHolder, serverIn))
      .collect(extractTextAndDest)
      .take(messagesToSend.size)
      .drainTo(Drain.fromSubscriber(new JmsSender(connectionHolder, replyTo(string2textMessage))))

    val clientRequests =
      new JmsSender[String](connectionHolder, serverIn, enrichReplyTo(clientIn)(string2textMessage))
    Spout(messagesToSend).drainTo(Drain.fromSubscriber(clientRequests))

    whenReady(result) { _ == messagesToSend }
  }

  def extractText: PartialFunction[Message, String] = {
    case msg: TextMessage => msg.getText
  }

  def extractTextAndDest: PartialFunction[Message, (String, JmsDestination)] = {
    case msg: TextMessage => (msg.getText, msg.getJMSReplyTo)
  }

  override protected def afterAll(): Unit = env.shutdown()
}