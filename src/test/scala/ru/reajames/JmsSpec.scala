package ru.reajames

import Jms._
import scala.util._
import org.scalatest._
import org.apache.activemq._
import javax.jms.{Destination => JmsDestination, _}

/**
  * Specification on Jms.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 20.12.16 0:36.
  */
trait JmsSpec extends Matchers with JmsUtilities { this: FlatSpec =>
  def connectionFactory: ConnectionFactory
  def failingConnectionFactory: ConnectionFactory

  "Jms" should "sucessfully create connection" in {
    connection(connectionFactory) should matchPattern {
      case Success(connection) =>
    }
  }

  it should "successfully start and stop previously created connection" in {
    (for {
      c <- connection(connectionFactory)
      _ <- start(c)
      _ <- stop(c)
      _ <- close(c)
    } yield ()) should matchPattern {
      case Success(_) =>
    }
  }

  it should "fail to create connection" in {
    connection(new ActiveMQConnectionFactory("tcp://non-existent-host:61616")) should matchPattern {
      case Failure(th) =>
    }
  }

  it should "create destinations and message to it" in {
    for {
      c <- connection(connectionFactory)
      s <- session(c)
      destinationFactory <- List(
        Queue("queue-150"), Topic("topic"), TemporaryQueue(), TemporaryTopic(), Destination(TemporaryQueue()(s))
      )
      destination <- destination(s, destinationFactory)
      p <- producer(s)
    } {
      destination shouldBe a[JmsDestination]
      send(p, s.createTextMessage("message to be sent"), destination).isSuccess should equal(true)
    }
  }

  it should "create a message consumer" in {
    (for {
      c <- connection(connectionFactory)
      session <- session(c)
      destination <- destination(session, Queue("queue"))
      consumer <- consumer(session, destination)
      _ <- close(c)
    } yield consumer) should matchPattern {
      case Success(consumer: MessageConsumer) =>
    }
  }

  it should "send a message to the specified destination" in {
    val q = Queue("specified-destination-6546")
    for {
      c <- connection(connectionFactory)
      _ <- start(c)
      s <- session(c)
      p <- producer(s)
      d <- destination(s, q)
      m <- Try(s.createTextMessage("created message"))
      _ <- send(p, m)
      cons <- consumer(s, d)
      received <- receive(cons)
      _ <- close(c)
    } {
      received should matchPattern {
        case Some(msg: TextMessage) if msg.getText == "created message" =>
      }
    }
  }

  it should "consume messages published to a destination" in {
    val messages = (1 to 10).map(_.toString).toList
    for {
      c <- connection(connectionFactory)
      s <- session(c)
      d <- destination(s, Queue("queue-100"))
      p <- producer(s, d)
    } {
      messages.map(string2textMessage(s, _)).foreach(send(p, _))
      close(c)
    }

    for {
      c <- connection(connectionFactory)
      _ <- start(c)
      s <- session(c)
      d <- destination(s, Queue("queue-100"))
      consumer <- consumer(s, d)
    } {
      messages.map(_ -> receive(consumer)).collect {
        case (i, Success(Some(message: TextMessage))) =>
          message.getText
      } should equal(messages)
      close(c)
    }
  }
}
