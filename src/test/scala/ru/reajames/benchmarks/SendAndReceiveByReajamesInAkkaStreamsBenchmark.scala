package ru.reajames
package benchmarks

import akka.stream.scaladsl._
import akka.actor.ActorSystem
import scala.language.postfixOps
import javax.jms.{Message, TextMessage}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import scala.concurrent.{ExecutionContext, Future}

/**
  * Benchmark on producing and consumption by reajames components in akka streams.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
object SendAndReceiveByReajamesInAkkaStreamsBenchmark extends SendAndReceiveBenchmark {
  implicit val system = ActorSystem("benchmark")
  implicit val mat = ActorMaterializer(ActorMaterializerSettings(system).withDispatcher("akka.stream.default-blocking-io-dispatcher"))

  def useCaseName: String = "Send and receive by Reajames in Akka Streams"

  override def useCase = new SendAndReceive {
    def connectionHolder: ConnectionHolder = connection
    override implicit def executionContext: ExecutionContext = system.dispatchers.lookup("akka.stream.default-blocking-io-dispatcher")

    override def sendMessages(messages: List[String]): Future[Unit] = {
      Source(messages)
        .alsoTo(Sink.fromSubscriber(new JmsSender[String](connectionHolder, queue, string2textMessage)))
        .runWith(Sink.ignore).map(_ => ())
    }

    override def receiveMessages(n: Long): Future[List[String]] = {
        Source.fromPublisher(new JmsReceiver(connectionHolder, queue))
          .take(n)
          .collect(extractText)
          .runWith(Sink.seq)
          .map(_.toList)
    }
  }

  override def after: Unit = system.terminate()

  def extractText: PartialFunction[Message, String] = {
    case msg: TextMessage => msg.getText
  }
}