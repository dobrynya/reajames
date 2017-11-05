package ru.reajames

import javax.jms.Message
import concurrent.duration._
import scala.language.postfixOps
import java.util.concurrent.Executors
import swave.core.{Drain, Pipe, Spout, StreamEnv}
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Tests Reajames in Swave.
  */
class SwaveTests extends ReajamesStreamTests {
  behavior of "Reajames in Swave"

  implicit val env: StreamEnv = StreamEnv()
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newScheduledThreadPool(12))

  def receiveMessages(receiver: JmsReceiver, messagesToReceive: Int): Future[List[String]] =
    receiveMessages(receiver, messagesToReceive, extractText)

  def receiveMessages[T](receiver: JmsReceiver, messagesToReceive: Int, extractor: PartialFunction[Message, T]): Future[List[T]] =
    Spout.fromPublisher(receiver).take(messagesToReceive).collect(extractor).drainToList(messagesToReceive)

  def sendMessages(sender: JmsSender[String], messages: List[String]): Unit =
    Spout(messages).drainTo(Drain.fromSubscriber(sender))

  def pipeline[T](receiver: JmsReceiver, messageAmount: Int, sender: JmsSender[T])(mapper: PartialFunction[Message, T]): Unit =
    Spout.fromPublisher(receiver)
      .take(messageAmount)
      .collect(mapper)
      .drainTo(Drain.fromSubscriber(sender))

  def sendThroughProcessor(messages: Iterator[String], sender: JmsSender[String]): Future[Unit] =
    Spout.fromIterator(messages)
      .via(Pipe.fromProcessor(sender))
      .drainToBlackHole()

  def delayFor100ms: Future[_] = {
    val promise = Promise[Unit]
    env.scheduler.scheduleOnce(100 millis, new Runnable {
      def run(): Unit = promise.success(())
    })
    promise.future
  }

  override protected def afterAll(): Unit = env.shutdown()
}
