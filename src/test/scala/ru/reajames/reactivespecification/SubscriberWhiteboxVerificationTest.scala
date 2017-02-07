package ru.reajames
package reactivespecification

import org.scalatest.testng.TestNGSuiteLike
import java.util.concurrent.atomic.AtomicInteger
import concurrent.ExecutionContext.Implicits.global
import org.reactivestreams.{Subscriber, Subscription}
import org.reactivestreams.tck.{SubscriberWhiteboxVerification, TestEnvironment}
import org.reactivestreams.tck.SubscriberWhiteboxVerification.{SubscriberPuppet, WhiteboxSubscriberProbe}

/**
  * Tests JmsSender using Reactive Streams TCK.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 05.02.17 11:26.
  */
class SubscriberWhiteboxVerificationTest extends SubscriberWhiteboxVerification[String](new TestEnvironment(300)) with TestNGSuiteLike
  with ActimeMQConnectionFactoryAware {

  val connectionHolder = new ConnectionHolder(connectionFactory)
  val counter = new AtomicInteger()

  private def queue = Queue("subscriber-bb-verification-" + counter.incrementAndGet)

  def createSubscriber(probe: WhiteboxSubscriberProbe[String]): Subscriber[String] = {
    new JmsSender[String](connectionHolder, queue, _ createTextMessage _) {
      override def onSubscribe(subscription: Subscription): Unit = {
        super.onSubscribe(subscription)
        probe.registerOnSubscribe(new SubscriberPuppet {
          def triggerRequest(elements: Long) = subscription.request(elements)
          def signalCancel() = subscription.cancel()
        })
      }

      override def onNext(element: String): Unit = {
        super.onNext(element)
        probe.registerOnNext(element)
      }

      override def onComplete(): Unit = {
        super.onComplete()
        probe.registerOnComplete()
      }

      override def onError(th: Throwable): Unit = {
        super.onError(th)
        probe.registerOnError(th)
      }
    }
  }

  def createElement(element: Int): String = "message " + element
}
