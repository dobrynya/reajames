package ru.reajames
package benchmarks

import scala.language.postfixOps

/**
  * Benchmark on producing and consumption by JMS components only.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.01.17 19:24.
  */
object SendAndReceiveJmsOnlyBenchmark extends SendAndReceiveBenchmark {
  def useCaseName: String = "JMS Only"
}