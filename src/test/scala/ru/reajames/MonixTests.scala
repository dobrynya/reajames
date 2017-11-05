package ru.reajames

import javax.jms.Message
import monix.reactive.Observable
import scala.language.postfixOps
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import monix.reactive.observers.Subscriber
import monix.execution.{Cancelable, Scheduler}
import org.scalatest.exceptions.TestCanceledException

/**
  * Tests Reajames in Monix.
  */
class MonixTests extends ReajamesStreamTests {
  behavior of "Reajames in Monix"

  implicit val executionContext: Scheduler = monix.execution.Scheduler.Implicits.global

  def doNothingWhenCancelled(): Cancelable = () => ()

  def receiveMessages(receiver: JmsReceiver, messagesToReceive: Int): Future[List[String]] =
    receiveMessages(receiver, messagesToReceive, extractText)

  def receiveMessages[T](receiver: JmsReceiver, messagesToReceive: Int, extractor: PartialFunction[Message, T]): Future[List[T]] =
    Observable.fromReactivePublisher(receiver).take(messagesToReceive).collect(extractor).toListL.runAsync

  def sendMessages(sender: JmsSender[String], messages: List[String]): Unit =
    Observable(messages : _*).subscribe(Subscriber.fromReactiveSubscriber(sender, doNothingWhenCancelled))

  def pipeline[T](receiver: JmsReceiver, messageAmount: Int, sender: JmsSender[T])(mapper: PartialFunction[Message, T]): Unit =
    Observable.fromReactivePublisher(receiver)
      .take(messageAmount)
      .collect(mapper)
      .subscribe(Subscriber.fromReactiveSubscriber(sender, doNothingWhenCancelled))

  def sendThroughProcessor(messages: Iterator[String], sender: JmsSender[String]): Future[Unit] = {
    throw new TestCanceledException("Monix does not support the required functionality!", -1)
  }

  def delayFor100ms: Future[_] = {
    val promise = Promise[Unit]
    executionContext.scheduleOnce(100 millis) {
      promise.success(())
    }
    promise.future
  }
}
