package ru.reajames.benchmarks

import swave.core._
import ru.reajames._
import ru.reajames.Jms._
import org.scalameter.api._
import scala.concurrent.Await
import scala.language.postfixOps
import scala.concurrent.duration._
import javax.jms.{Message, TextMessage}
import org.scalameter.picklers.Implicits._

/**
  * Benchmark on producing and consumption by reajames components.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
object SendAndReceiveMessages extends Bench[Double] with ActimeMQConnectionFactoryAware {
  lazy val executor = LocalExecutor(new Executor.Warmer.Default, Aggregator.min[Double], measurer)
  lazy val measurer = new Measurer.Default
  lazy val reporter = new LoggingReporter[Double]
  lazy val persistor = Persistor.None

  val messages =
    for (amount <- Gen.range("messageAmount")(1000, 35000, 5000))
      yield (1 to amount).map(_.toString).toList

  implicit val env = StreamEnv()
  import env.defaultDispatcher

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

    val result = Spout
      .fromPublisher(new JmsReceiver(connectionHolder, in))
      .take(messages.size)
      .drainToList(messages.size)

    Spout.fromIterable(messages)
      .drainTo(Drain.fromSubscriber(new JmsSender[String](connectionHolder,
        permanentDestination(in)(string2textMessage))))

    Await.ready(result, 5 minutes)

    def extractText: PartialFunction[Message, String] = {
      case msg: TextMessage => msg.getText
    }
  }
}