package ru.reajames
package benchmarks

import scala.concurrent.Future
import scala.language.postfixOps

/**
  * Benchmark on sending by reajames.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
object SendByReajamesBenchmark extends SendAndReceiveBenchmark {
  def useCaseName: String = "Send by Reajames"

  override def useCase = new SendAndReceive {
    def connectionHolder: ConnectionHolder = connection

    override def sendMessages(messages: List[String]): Future[Unit] = Future {
      QueuePublisher(messages).subscribe(
        new JmsSender[String](connectionHolder, permanentDestination(queue)(string2textMessage))
      )
      logger.debug("Sent {} messages", messages.size)
    }
  }
}