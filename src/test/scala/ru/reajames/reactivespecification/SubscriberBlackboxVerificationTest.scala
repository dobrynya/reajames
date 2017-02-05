package ru.reajames
package reactivespecification

import org.reactivestreams.Subscriber
import org.scalatest.testng.TestNGSuiteLike
import java.util.concurrent.atomic.AtomicInteger
import concurrent.ExecutionContext.Implicits.global
import org.reactivestreams.tck.{SubscriberBlackboxVerification, TestEnvironment}

/**
  * Tests JmsSender using Reactive Streams TCK.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 05.02.17 11:26.
  */
class SubscriberBlackboxVerificationTest extends SubscriberBlackboxVerification[String](new TestEnvironment()) with TestNGSuiteLike
  with ActimeMQConnectionFactoryAware {

  val connectionHolder = new ConnectionHolder(connectionFactory)
  val counter = new AtomicInteger()

  private def queue = Queue("subscriber-bb-verification-" + counter.incrementAndGet)

  def createSubscriber(): Subscriber[String] =
    new JmsSender[String](connectionHolder, queue, _ createTextMessage _)

  def createElement(element: Int): String = "message " + element
}
