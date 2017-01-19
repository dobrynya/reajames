package ru.reajames

import swave.core._
import scala.concurrent.Future
import org.scalatest._, concurrent._, time._
import javax.jms.{Destination, Message, TextMessage}

/**
  * Tests using JMS connectors in the Swave infrastructure.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 2:44.
  */
class JmsConnectorsInSwaveTest extends FlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures
  with ActimeMQConnectionFactoryAware with JmsUtilities {
  behavior of "JMS connectors"

  implicit val env = StreamEnv()
  import env.defaultDispatcher

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

    // Main pipeline
    Spout.fromPublisher(new JmsReceiver(connectionFactory, Queue("in"))).take(3)
      .collect {
        case msg: TextMessage => (msg.getText, msg.getJMSReplyTo) // store JMSReplyTo header with message body
      }
      .drainTo(Drain.fromSubscriber(new JmsSender[(String, Destination)](connectionFactory, replyTo(string2textMessage))))

    // just enriches a newly created text message with JMSReplyTo
    val sendReplyToHeader: DestinationAwareMessageFactory[String] =
      (session, elem) =>
        (session.createTextMessage(elem).tap(_.setJMSReplyTo(Queue(elem)(session))), Queue("in")(session))

    val sender = new JmsSender[String](connectionFactory, sendReplyToHeader)
    Spout(messagesToSend).drainTo(Drain.fromSubscriber(sender))

    whenReady(messagesReceivedByClients, timeout(Span(15, Seconds))) {
      _ == messagesToSend
    }
  }


  def extractText: PartialFunction[Message, String] = {
    case msg: TextMessage => msg.getText
  }

  override protected def afterAll(): Unit = env.shutdown()
}