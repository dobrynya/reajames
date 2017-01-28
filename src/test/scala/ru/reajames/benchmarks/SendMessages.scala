package ru.reajames.benchmarks

import org.slf4j._
import ru.reajames._
import org.scalameter.api._
import scala.concurrent.Future
import scala.language.postfixOps
import javax.jms.{Message, TextMessage}
import org.scalameter.picklers.Implicits._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Benchmark on sending by reajames.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
object SendMessages extends Bench[Double] with ActimeMQConnectionFactoryAware {
  lazy val executor = LocalExecutor(new Executor.Warmer.Default, Aggregator.min[Double], measurer)
  lazy val measurer = new Measurer.Default
  lazy val reporter = new LoggingReporter[Double]
  lazy val persistor = Persistor.None

  val messages =
    for (amount <- Gen.range("messageAmount")(1000, 35000, 5000))
      yield (1 to amount).map(_.toString).toList

  performance of "Reajames" in {
    measure method "send and receive all messages" in {
      using(messages) in { messagesToSend =>
        new UseCase(messagesToSend)
      }
    }
  }

  val connectionHolder = new ConnectionHolder(connectionFactory)

  val logger = LoggerFactory.getLogger(getClass)

  class UseCase(messages: List[String]) {
    private val size = messages.size
    logger.info("Starting sending and receiving {} messages", size)

    val out = Queue("performance-out")

    def send: Future[Unit] = Future {
      QueuePublisher(messages).subscribe(
        new JmsSender[String](connectionHolder, permanentDestination(out)(string2textMessage))
      )
      logger.info("Published {} messages", size)
    }

    send

    import Jms._

    for {
      c <- connectionHolder.connection
      s <- session(c)
      d <- destination(s, out)
      cons <- consumer(s, d)
    } {
      messages.foreach(m => receive(cons))
      close(cons)
    }

    logger.info("Received {} messages", size)

    def extractText: PartialFunction[Message, String] = {
      case msg: TextMessage => msg.getText
    }
  }
}