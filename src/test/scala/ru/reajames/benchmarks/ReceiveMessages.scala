package ru.reajames.benchmarks

import javax.jms.{Message, TextMessage}
import org.slf4j._
import ru.reajames._
import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Benchmark on consumption by reajames.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
object ReceiveMessages extends Bench[Double] with ActimeMQConnectionFactoryAware {
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

    val in = Queue("performance-in")

    def receive: Future[List[String]] = {
      val promise = Promise[List[String]]
      val result = List.newBuilder[String]
      result.sizeHint(messages)
      var counter = 0

      new JmsReceiver(connectionHolder, in).subscribe(new TestSubscriber(
        request = Some(size),
        next = (subs, msg) => {
          result += msg.asInstanceOf[TextMessage].getText
          counter += 1
          if (counter == size) {
            logger.info("Received {} messages", size)
            promise.success(result.result())
            subs.cancel()
          }
        }
      ))
      promise.future
    }

    import Jms._

    val result = receive

    for {
      c <- connectionHolder.connection
      s <- session(c)
      d <- destination(s, in)
      p <- producer(s)
    } {
      messages.foreach(m => p.send(d, s.createTextMessage(m)))
      close(p)
    }

    logger.info("Sent {} messages", size)

    Await.ready(result, 1 minute)

    def extractText: PartialFunction[Message, String] = {
      case msg: TextMessage => msg.getText
    }
  }
}