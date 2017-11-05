package ru.reajames

import javax.jms.Message
import language.postfixOps
import concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.ThrottleMode.Shaping
import akka.stream.scaladsl.{Flow, Sink, Source}
import scala.concurrent.{ExecutionContext, Future}

/**
  * Tests Reajames in Akka Stream.
  */
class AkkaTests extends ReajamesStreamTests {
  behavior of "Reajames in Akka"

  implicit val system: ActorSystem = ActorSystem("test")
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext =
    system.dispatchers.lookup("akka.stream.default-blocking-io-dispatcher")

  def receiveMessages(receiver: JmsReceiver, messagesToReceive: Int): Future[List[String]] =
    receiveMessages(receiver, messagesToReceive, extractText)

  def receiveMessages[T](receiver: JmsReceiver, messagesToReceive: Int,
                         extractor: PartialFunction[Message, T]): Future[List[T]] =
    Source.fromPublisher(receiver)
      .take(messagesToReceive)
      .collect(extractor)
      .runWith(Sink.seq)
      .map(_.toList)

  def sendMessages(sender: JmsSender[String], messages: List[String]): Unit =
    Source(messages).runWith(Sink.fromSubscriber(sender))

  def pipeline[T](receiver: JmsReceiver, messageAmount: Int, sender: JmsSender[T])(mapper: PartialFunction[Message, T]): Unit =
    Source.fromPublisher(receiver).take(messageAmount).collect(mapper)
      .runWith(Sink.fromSubscriber(sender))

  def sendThroughProcessor(messages: Iterator[String], sender: JmsSender[String]): Future[Unit] =
    Source.fromIterator(() => messages)
      .throttle(1, 100 millis, 1, Shaping)
      .via(Flow.fromProcessor(() => sender))
      .runWith(Sink.ignore).map(_ => ())

  def delayFor100ms: Future[_] =
    akka.pattern.after(100 millis, system.scheduler)(Future.successful(true))

  override protected def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }
}
