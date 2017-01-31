package ru.reajames
package benchmarks

import swave.core._
import scala.concurrent.Future
import scala.language.postfixOps
import javax.jms.{Message, TextMessage}

/**
  * Benchmark on producing and consumption by reajames components in akka streams.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
object SendAndReceiveByReajamesInSwaveBenchmark extends SendAndReceiveBenchmark {
  implicit val env = StreamEnv()

  def useCaseName: String = "Send and receive by Reajames in Akka Streams"

  override def useCase = new SendAndReceive {
    def connectionHolder: ConnectionHolder = connection

    override def sendMessages(messages: List[String]): Future[Unit] = {
      Spout(messages)
        .to(Drain.fromSubscriber(new JmsSender[String](connectionHolder, queue, string2textMessage))).run().termination
    }

    override def receiveMessages(n: Long): Future[List[String]] = {
        Spout.fromPublisher(new JmsReceiver(connectionHolder, queue))
          .take(n)
          .collect(extractText)
          .drainToList(n.toInt)
    }
  }


  override def after: Unit = env.shutdown()

  def extractText: PartialFunction[Message, String] = {
    case msg: TextMessage => msg.getText
  }
}