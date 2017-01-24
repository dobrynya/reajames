package ru.reajames.benchmarks

import javax.jms.{Message, TextMessage}

import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.slf4j.LoggerFactory
import ru.reajames.Jms._
import ru.reajames._

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Benchmark on producing and consumption by reajames components.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
object JmsSendAndReceiveMessages extends Bench[Double] with ActimeMQConnectionFactoryAware {
  lazy val executor = LocalExecutor(new Executor.Warmer.Default, Aggregator.min[Double], measurer)
  lazy val measurer = new Measurer.Default
  lazy val reporter = new LoggingReporter[Double]
  lazy val persistor = Persistor.None

  val messages =
    for (amount <- Gen.range("messageAmount")(1000, 35000, 5000))
      yield (1 to amount).map(_.toString).toList

  import concurrent.ExecutionContext.Implicits.global

  performance of "Reajames" in {
    measure method "send and receive all messages" in {
      using(messages) in { messagesToSend =>
        new UseCase(messagesToSend)

      }
    }
  }

  val connectionHolder = new ConnectionHolder(connectionFactory)

  class UseCase(messages: List[String]) {
    val in = Queue("performance-in")
    val logger = LoggerFactory.getLogger(getClass)

    logger.info("Sending and receiving {} messages", messages.size)

    Future {
      for {
        c <- connectionHolder.connection
        s <- session(c)
        d <- destination(s, in)
        p <- producer(s, d)
      } {
        messages.map { m =>
          logger.trace("Sending {}", m)
          send(p, s.createTextMessage(m))
        }
        close(p)
      }
    } onComplete {
      case Success(_) => logger.info("Finished sending messages")
      case Failure(th) => logger.warn("Stopped due to error!", th)
    }

    private val received: Try[List[String]] = for {
      c <- connectionHolder.connection
      s <- session(c)
      d <- destination(s, in)
      cons <- consumer(s, d)
    } yield {
      val res = messages.map { _ =>
        receive(cons, Some(150)) match {
          case Success(Some(m: TextMessage)) =>
            val r = m.getText
            logger.trace("Received {}", r)
            r
        }
      }
      close(cons)
      res
    }

    logger.info(received match {
      case Success(res) if res == messages => "Successfully received all messages"
      case _ => "Received messages do not equal sent messages!"
    })

    def extractText: PartialFunction[Message, String] = {
      case msg: TextMessage => msg.getText
    }
  }
}