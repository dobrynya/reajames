package ru.reajames
package reactivespecification

import javax.jms.Message
import scala.concurrent.Future
import org.reactivestreams.Publisher
import org.scalatest.testng.TestNGSuiteLike
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}

/**
  * Tests JmsReceiver using Reactive Streams TCK.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 31.01.17 14:15.
  */
class PublisherVerificationTest extends PublisherVerification[Message](new TestEnvironment(300)) with
  JmsUtilities with ActimeMQConnectionFactoryAware with TestNGSuiteLike {
  val counter = new AtomicInteger()

  def createPublisher(elements: Long): Publisher[Message] = {
    val queue = Queue("publisher-verification-%s" format counter.incrementAndGet())
    val messages = 1L to elements

    var shouldCancel = false

    Future {
      QueuePublisher[Long](messages, () => shouldCancel).subscribe(new JmsSender(holder, queue, _ createTextMessage _.toString))
    }

    stopper(new JmsReceiver(holder, queue), elements.toLong, () => shouldCancel = true)
  }

  def createFailedPublisher(): Publisher[Message] =
    new JmsReceiver(new ConnectionHolder(failingConnectionFactory), Queue("failed"))
}