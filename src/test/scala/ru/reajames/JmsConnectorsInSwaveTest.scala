package ru.reajames

import swave.core._
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import org.scalatest._, concurrent._, time._
import javax.jms.{Message, TextMessage}

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

  "JmsReceiver" should "receive messages and pass it to stream for processing" in {
    val queue = Queue("queue-10")
    val messagesToBeSent = (1 to 50).map(_.toString).toList

    val receiver = new JmsReceiver(connectionFactory, queue)
    val received = Spout.fromPublisher(receiver).collect(extractText).take(25).drainToList(50)

    sendMessages(messagesToBeSent, string2textMessage, queue)

    whenReady(received) {
      _ == messagesToBeSent.take(25)
    }
  }

  "Jms connectors" should "send and receive messages processing it within akka stream" in {
    val queue = Queue("queue-11")

    val messagesToSend = (1 to 100).map(_.toString).toList

    val received = Spout.fromPublisher(new JmsReceiver(connectionFactory, queue)).collect {
      case msg: TextMessage => msg.getText
    }.take(messagesToSend.size).drainToList(100)

    val sender = new JmsSender[String](connectionFactory, permanentDestination(queue)(string2textMessage))
    Spout(messagesToSend).drainTo(Drain.fromSubscriber(sender))

    whenReady(received, timeout(Span(10, Seconds))) {
      _ == messagesToSend
    }
  }

  "JmsSender" should "allow to send to an appropriate JMS destination" in {
    val messagesToSend = List("queue-11-1", "queue-11-2", "queue-11-3")

    val received = Future.sequence(
      messagesToSend
        .map(q => new JmsReceiver(connectionFactory, Queue(q)))
        .map(Spout.fromPublisher).map(_.collect(extractText).take(1).drainToHead())
    )

    // Using a message as a queue name to send message to
    val sendMessagesToDifferentQueues: DestinationAwareMessageFactory[String] =
      (session, elem) => (session.createTextMessage(elem), Queue(elem)(session))

    val sender = new JmsSender[String](connectionFactory, sendMessagesToDifferentQueues)
    Spout(messagesToSend).drainTo(Drain.fromSubscriber(sender))

    whenReady(received, timeout(Span(10, Seconds))) {
      _ == messagesToSend
    }
  }

  "Jms pipeline" should "respond to destination specified in the JMSReplyTo header" in {
    val messagesToSend = List("queue-11-4", "queue-11-5", "queue-11-6")

    val messagesReceivedByClients = Future.sequence(
      messagesToSend
        .map(q => new JmsReceiver(connectionFactory, Queue(q)))
        .map(Spout.fromPublisher).map(_.collect(extractText).take(1).drainToHead())
    )

    val serverIn = Queue("queue-11-in")

    // Main pipeline
    Spout.fromPublisher(new JmsReceiver(connectionFactory, serverIn)).take(3).collect {
      case msg: TextMessage => (msg.getText, msg.getJMSReplyTo)
    }.drainTo(Drain.fromSubscriber(new JmsSender(connectionFactory, replyTo(string2textMessage))))

    // just enriches a newly created text message with JMSReplyTo
    val sendReplyToHeader: DestinationAwareMessageFactory[String] =
      (session, elem) =>
        (session.createTextMessage(elem).tap(_.setJMSReplyTo(Queue(elem)(session))), serverIn(session))

    val sender = new JmsSender[String](connectionFactory, sendReplyToHeader)
    Spout(messagesToSend).drainTo(Drain.fromSubscriber(sender))

    whenReady(messagesReceivedByClients, timeout(Span(10, Seconds))) {
      _ == messagesToSend
    }
  }

  "Jms components" should "create a channel through a temporary queue" in {
    val messagesToSend = List("message 1", "message 2", "message 3")

    val serverIn = Queue("swave-q-1")
    val clientIn = Queue("swawe-q-2")

    // listen to a temporary queue
    val result = Spout.fromPublisher(new JmsReceiver(connectionFactory, clientIn))
      .collect(extractText)
      .take(messagesToSend.size)
      .onElement(s => println("Client received %s" format s))
      .drainToList(messagesToSend.size)

    def enrichReplyTo[T](replyTo: DestinationFactory)
                        (messageFactory: DestinationAwareMessageFactory[T]): DestinationAwareMessageFactory[T] =
      (session, element) =>
        messageFactory(session, element) match {
          case (msg, destination) => (msg.tap(_.setJMSReplyTo(replyTo(session))), destination)
        }

    Spout.fromPublisher(new JmsReceiver(connectionFactory, serverIn)).collect {
      case msg: TextMessage => (msg.getText, msg.getJMSReplyTo)
    }.take(messagesToSend.size).drainTo(Drain.fromSubscriber(new JmsSender(connectionFactory, replyTo(string2textMessage))))

    val clientRequests = new JmsSender[String](connectionFactory,
      enrichReplyTo(clientIn)(permanentDestination(serverIn)(string2textMessage)))
    Spout(messagesToSend).drainTo(Drain.fromSubscriber(clientRequests))

    whenReady(result, timeout(Span(10, Seconds))) {
      _ == messagesToSend
    }
  }

  def extractText: PartialFunction[Message, String] = {
    case msg: TextMessage => msg.getText
  }

  override protected def afterAll(): Unit = env.shutdown()
}