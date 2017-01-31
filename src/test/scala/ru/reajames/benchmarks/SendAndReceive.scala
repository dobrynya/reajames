package ru.reajames
package benchmarks

import Jms._
import scala.util.Success
import javax.jms.TextMessage
import scala.language.postfixOps
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Provides abilities to send and receive messages.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 30.01.17 23:40.
  */
trait SendAndReceive extends Logging {
  def connectionHolder: ConnectionHolder
  implicit def executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val queue = Queue("performance-in")

  def sendAndReceive(messages: List[String]): Unit = {
    logger.debug("Sending {} messages", messages.size)
    val receiving = receiveMessages(messages.size)
    val sending = sendMessages(messages)

    val result =
      for (received <- receiving)  yield {
        if (received == messages)
          logger.info("Received messages correspond sent messages")
        else
          logger.warn("Received messages don't correspond sent messages!")
      }

    Await.ready(result, 1 minute)
  }

  def sendMessages(messages: List[String]): Future[Unit] = Future {
    for {
      c <- connectionHolder.connection
      s <- session(c)
      d <- destination(s, queue)
      p <- producer(s)
    } {
      messages.map { m =>
        logger.trace("Sending {}", m)
        send(p, s.createTextMessage(m), d)
      }
      logger.debug("Sent {} messages", messages.size)
      close(p)
    }
  }

  def receiveMessages(n: Long): Future[List[String]] = Future {
    (for {
      c <- connectionHolder.connection
      s <- session(c)
      d <- destination(s, queue)
      cons <- consumer(s, d)
    } yield {
      val res = (1L to n).map(_ => receive(cons)).collect {
        case Success(Some(msg: TextMessage)) =>
          val text = msg.getText
          logger.trace("Received {}", text)
          text
      }
      close(cons)
      res
    }).get.toList
  }
}
