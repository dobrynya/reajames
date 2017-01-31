package ru.reajames
package benchmarks

import org.scalameter.api._
import scala.language.postfixOps
import org.scalameter.picklers.Implicits._

/**
  * Benchmark on producing and consumption.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
trait SendAndReceiveBenchmark extends Bench[Double] with ActimeMQConnectionFactoryAware { self =>
  lazy val executor = LocalExecutor(new Executor.Warmer.Default, Aggregator.min[Double], measurer)
  lazy val measurer = new Measurer.Default
  lazy val reporter = new LoggingReporter[Double]
  lazy val persistor = Persistor.None

  val connection = new ConnectionHolder(connectionFactory)
  val messages =
    for (amount <- Gen.range("messageAmount")(5000, 30000, 5000)) yield (1 to amount).map(_.toString).toList

  def useCaseName: String

  performance of useCaseName in {
    measure method "send and receive all messages" in {
      using(messages) in { messagesToSend =>
       useCase.sendAndReceive(messagesToSend)
      }
    }
  }

  def after: Unit = ()

  override def executeTests(): Boolean = {
    val result = super.executeTests()
    after
    result
  }

  def useCase: SendAndReceive =  new SendAndReceive {
    def connectionHolder: ConnectionHolder = connection
  }
}