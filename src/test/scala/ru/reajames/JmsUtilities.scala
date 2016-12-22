package ru.reajames

import Jms._
import javax.jms.ConnectionFactory

/**
  * Provides helpful utilities for testing purposes.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 2:03.
  */
trait JmsUtilities {

  /**
    * Sends messages to the specified destination and closes connection.
    * @param messages messages to be sent
    * @param messageFactory a factory to convert messages
    * @param destinationFactory a destination
    * @param connectionFactory connection factory
    * @tparam T data element type
    */
  def sendMessages[T](messages: Traversable[T], messageFactory: MessageFactory[T],
                      destinationFactory: DestinationFactory)
                     (implicit connectionFactory: ConnectionFactory): Unit = {
    for {
      c <- connection(connectionFactory)
      s <- session(c)
      d <- destination(s, destinationFactory)
      p <- producer(s, d)
    } {
      messages.foreach(m => send(p, messageFactory(s)(m)))
      close(c)
    }
  }
}
