package ru.reajames

import javax.jms
import org.scalatest._

/**
  * Specification on destinations.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 06.07.17 23:09.
  */
class DestinationSpec extends FlatSpec with Matchers {
  behavior of "Destinations"

  it should "generate string representations" in {
    Topic("topic").toString should startWith("Topic")
    Queue("queue").toString should startWith("Queue")
    TemporaryTopic().toString should startWith("TemporaryTopic")
    TemporaryQueue().toString should startWith("TemporaryQueue")
    Destination(new jms.Destination {
      override def toString: String = "Destination"
    }).toString should equal("Destination")
  }
}