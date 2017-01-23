package ru.reajames.benchmarks

import java.util.concurrent.TimeUnit
import javax.jms.{Message, TextMessage, Destination => JmsDestination}

import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import ru.reajames._
import swave.core.{Drain, Spout, StreamEnv}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/**
  * Benchmark on producing and consumption by reajames components.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
object ChannelBenchmark extends Bench[Double] with ActimeMQConnectionFactoryAware {
  lazy val executor = LocalExecutor(new Executor.Warmer.Default, Aggregator.min[Double], measurer)
  lazy val measurer = new Measurer.Default
  lazy val reporter = new LoggingReporter[Double]
  lazy val persistor = Persistor.None

  val messageAmount = Gen.range("messageAmount")(1000, 35000, 5000)
  val messages =
    for (amount <- messageAmount)
      yield (1 to amount).map(_.toString).toList

  performance of "Reajames" in {
    measure method "send and receive all messages" in {
      using(messages) in { messagesToSend =>
        new Channel(messagesToSend)
      }
    }
  }

  val connectionHolder = new ConnectionHolder(connectionFactory)

  class Channel(messages: List[String]) {

    implicit val env = StreamEnv()
    import env.defaultDispatcher

    val serverIn = Queue("performance-in")
    val clientIn = TemporaryQueue()

    val result =
      Spout
        .fromPublisher(new JmsReceiver(connectionHolder, clientIn))
        .collect(extractText)
        .take(messages.size)
        .drainToList(messages.size)

    Spout
      .fromPublisher(new JmsReceiver(connectionHolder, serverIn))
      .collect(extractTextAndReplyTo)
      .take(messages.size)
      .drainTo(Drain.fromSubscriber(new JmsSender(connectionHolder, replyTo(string2textMessage))))

    val clientRequests = new JmsSender[String](connectionHolder,
      enrichReplyTo(clientIn)(permanentDestination(serverIn)(string2textMessage)))
    Spout(messages).drainTo(Drain.fromSubscriber(clientRequests))

    Await.ready(result, FiniteDuration(5, TimeUnit.MINUTES))
    result onComplete {
      case Success(res) =>
        println("Resulted with %s messages".format(res.size))
      case Failure(th) =>
        th.printStackTrace()
    }

    env.shutdown()

    def extractTextAndReplyTo: PartialFunction[Message, (String, JmsDestination)] = {
      case msg: TextMessage => (msg.getText, msg.getJMSReplyTo)
    }

    def extractText: PartialFunction[Message, String] = {
      case msg: TextMessage => msg.getText
    }
  }
}