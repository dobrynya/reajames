package ru.reajames
package benchmarks

import javax.jms.TextMessage
import scala.concurrent.{Future, Promise}
import scala.language.postfixOps

/**
  * Benchmark on sending and consumption by Reajames.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
object SendAndReceiveByReajamesBenchmark extends SendAndReceiveBenchmark {
  def useCaseName: String = "Receive by Reajames"

  override def useCase = new SendAndReceive {
    def connectionHolder: ConnectionHolder = connection

    override def receiveMessages(n: Long): Future[List[String]] = {
      val promise = Promise[List[String]]
      val result = List.newBuilder[String]
      result.sizeHint(n.toInt)
      var counter = 0L

      new JmsReceiver(connectionHolder, queue).subscribe(TestSubscriber(
        request = Some(n),
        next = (subs, msg) => {
          result += msg.asInstanceOf[TextMessage].getText
          counter += 1
          if (counter == n) {
            promise.success(result.result())
            subs.cancel()
          }
        }
      ))
      promise.future
    }

    override def sendMessages(messages: List[String]): Future[Unit] = Future {
      QueuePublisher(messages).subscribe(
        new JmsSender[String](connectionHolder, queue, string2textMessage)
      )
      logger.debug("Sent {} messages", messages.size)
    }
  }
}