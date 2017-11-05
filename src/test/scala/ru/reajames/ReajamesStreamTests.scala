package ru.reajames

import org.scalatest._
import ReajamesStreamTests._
import org.scalatest.concurrent.ScalaFutures
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}
import org.scalatest.time.{Milliseconds, Seconds, Span}
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import javax.jms.{Message, Session, TextMessage, Destination => JDestination}

/**
  * Common tests for testing in a stream environment.
  */
trait ReajamesStreamTests extends FlatSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures
  with FfmqConnectionFactoryAware {

  val connectionHolder = new ConnectionHolder(connectionFactory, Some("embedded-amq"))

  implicit def executionContext: ExecutionContext

  def receiveMessages(receiver: JmsReceiver, messagesToReceive: Int = 1): Future[List[String]]

  def receiveMessages[T](receiver: JmsReceiver, messagesToReceive: Int, extractor: PartialFunction[Message, T]): Future[List[T]]

  def sendMessages(sender: JmsSender[String], messages: List[String]): Unit

  def pipeline[T](receiver: JmsReceiver, messageAmount: Int, sender: JmsSender[T])(mapper: PartialFunction[Message, T]): Unit

  def sendThroughProcessor(messages: Iterator[String], sender: JmsSender[String]): Future[Unit]

  def delayFor100ms: Future[_]

  "Jms connectors" should "send and receive messages processing it within a stream" in {
    val queue = Queue("send-and-receive" + counter.getAndIncrement())

    val messagesToSend = (1 to 10).map(_.toString).toList

    val received = receiveMessages(new JmsReceiver(connectionHolder, queue), messagesToSend.size)
    sendMessages(new JmsSender[String](connectionHolder, permanentDestination(queue)(string2textMessage)), messagesToSend)

    whenReady(received, timeout(Span(30, Seconds))) {
      _ == messagesToSend
    }
  }

  "JmsSender" should "allow to send to an appropriate JMS destination" in {
    val messagesToSend = List.fill(3)("jms-destination-routing-" + counter.getAndIncrement())

    val received = Future.sequence(
      messagesToSend.map(q => receiveMessages(new JmsReceiver(connectionHolder, Queue(q))))
    )

    // Using a message as a queue name to send message to
    val sendMessagesToDifferentQueues: DestinationAwareMessageFactory[String] =
      (session, elem) => (session.createTextMessage(elem), Queue(elem)(session))

    sendMessages(new JmsSender[String](connectionHolder, sendMessagesToDifferentQueues), messagesToSend)

    whenReady(received, timeout(Span(30, Seconds))) {
      _ == messagesToSend
    }
  }

  "Jms pipeline" should "respond to destination specified in the JMSReplyTo header" in {
    val messagesToSend = List.fill(3)("reply-to-" + counter.getAndIncrement())

    val messagesReceivedByClients = Future.sequence(
      messagesToSend.map(q => receiveMessages(new JmsReceiver(connectionHolder, Queue(q))))
    )

    val serverIn = Queue("in-" + counter.getAndIncrement())
    // Main pipeline
    val mainReceiver = new JmsReceiver(connectionHolder, serverIn)
    val mainSender = new JmsSender[(String, JDestination)](connectionHolder, replyTo(string2textMessage))
    pipeline(mainReceiver, messagesToSend.size, mainSender)(extractTextAndJMSReplyTo)

    // just enriches a newly created text message with JMSReplyTo
    val replyToElemName = (session: Session, elem: String) =>
      mutate(session.createTextMessage(elem))(_.setJMSReplyTo(Queue(elem)(session)))

    sendMessages(new JmsSender(connectionHolder, serverIn, replyToElemName), messagesToSend)

    whenReady(messagesReceivedByClients, timeout(Span(30, Seconds))) {
      _ == messagesToSend
    }
  }

  "Jms components" should "create a channel through a temporary queue" in {
    val messagesToSend = List("message 1", "message 2", "message 3")

    val serverIn = Queue("server-in-" + counter.getAndIncrement())
    val clientIn = TemporaryQueue(Some("client-in"))

    val result =
      receiveMessages(new JmsReceiver(connectionHolder, clientIn), messagesToSend.size)

    pipeline(new JmsReceiver(connectionHolder, serverIn), messagesToSend.size,
      new JmsSender[(String, JDestination)](connectionHolder, replyTo(string2textMessage)))(extractTextAndJMSReplyTo)

    val clientRequests = new JmsSender[String](connectionHolder, serverIn, enrichReplyTo(clientIn)(string2textMessage))
    sendMessages(clientRequests, messagesToSend)

    whenReady(result, timeout(Span(30, Seconds))) { _ == messagesToSend }
  }

  "ReajamesStreamTests" should "delay future completion for 100 millis" in {
    for (i <- 1 to 2) {
      val delayedFor100ms = delayFor100ms
      whenReady(delayedFor100ms, timeout = timeout(Span(300, Milliseconds)))( _ => ())
    }
  }

  "JmsSender" should "allow to detect a failure to recreate a sending stream" in {
    val queue = Queue("recreate-sending-stream-" + counter.getAndIncrement())

    def createSendingStream(messages: Iterator[String]): Future[Unit] = {
      val mf: MessageFactory[String] =
        (session, element) =>
          if (element != "1") session.createTextMessage(element)
          else throw new IllegalArgumentException(s"Could not send $element due to the test case!")

      sendThroughProcessor(messages, new JmsSender(connectionHolder, queue, mf)).recoverWith {
        case th => delayFor100ms.flatMap(_ => createSendingStream(messages))
      }
    }

    val sent = createSendingStream(Iterator("1", "2"))
    whenReady(sent, timeout(Span(30, Seconds)))(_ should equal(()))

    whenReady(receiveMessages(new JmsReceiver(connectionHolder, queue)),
      timeout(Span(30, Seconds)))(_ == List("2"))
  }

  "JmsReceiver" should "allow to detect a failure to recreate a receiving stream" in {
    val queue = Queue("recreate-receiving-stream-" + counter.getAndIncrement())

    def createReceivingStream(messagesToReceive: Int): Future[Seq[String]] = {
      receiveMessages(new JmsReceiver(connectionHolder, queue), messagesToReceive, {
        case text: TextMessage =>
          if (text.getText != "1") text.getText
          else throw new IllegalArgumentException(s"Could not process ${text.getText} due to the test case!")
      }).recoverWith {
        case th => delayFor100ms.flatMap(_ => createReceivingStream(messagesToReceive))
      }
    }

    sendMessages(new JmsSender(connectionHolder, queue, string2textMessage), List("1"))
    val received = createReceivingStream(1)

    Thread.sleep(300) // sleeping for propagating a failure in a stream

    sendMessages(new JmsSender(connectionHolder, queue, string2textMessage), List("2"))

    whenReady(received, timeout(Span(30, Seconds)))(_ == List("2"))
  }


  override protected def afterAll(): Unit = stopBroker()

  /**
    * Helper function for extracting text and JMSReplyTo header.
 *
    * @return payload and JMSReplyTo
    */
  def extractTextAndJMSReplyTo: PartialFunction[Message, (String, JDestination)] = {
    case msg: TextMessage => (msg.getText, msg.getJMSReplyTo)
  }

  /**
    * Helper function for extracting payload of a message.
    * @return text
    */
  def extractText: PartialFunction[Message, String] = {
    case msg: TextMessage => msg.getText
  }
}

object ReajamesStreamTests {
  private[reajames] val counter = new AtomicInteger(0)

  private val executor: ExecutorService = Executors.newCachedThreadPool(createDaemon)

  implicit val executionContext = ExecutionContext.fromExecutorService(executor)

  private def createDaemon = new ThreadFactory {
    def newThread(r: Runnable): Thread = {
      val thread = new Thread(r)
      thread.setDaemon(true)
      thread
    }
  }
}