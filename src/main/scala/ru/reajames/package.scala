package ru

import javax.jms._

/**
  * Contains helpful types and functions.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 20.12.16 23:55.
  */
package object reajames {

  /**
    * Creates a destination using specified session.
    */
  type DestinationFactory = Session => Destination

  /**
    * Creates a queue.
    * @param name specifies name of a queue
    */
  case class Queue(name: String) extends DestinationFactory {
    def apply(session: Session): Destination = session.createQueue(name)
  }

  /**
    * Creates a topic.
    * @param name specifies name of a topic
    */
  case class Topic(name: String) extends DestinationFactory {
    def apply(session: Session): Destination = session.createTopic(name)
  }

  /**
    * Creates a temporary unnamed queue.
    */
  case object TemporaryQueue extends DestinationFactory {
    def apply(session: Session): Destination = session.createTemporaryQueue()
  }

  /**
    * Creates a temporary unnamed topic.
    */
  case object TemporaryTopic extends DestinationFactory {
    def apply(session: Session): Destination = session.createTemporaryTopic()
  }

  /**
    * Creates a message using specified data element and session.
    * @tparam T specifies data type
    */
  type MessageFactory[T] = Session => T => Message

  /**
    * Creates a text message using session and a string.
    */
  val string2textMessage: MessageFactory[String] = session => text => session.createTextMessage(text)

  /**
    * Parses a message to interpret it as a data type instance.
    * @tparam T specifies data type
    */
  type MessageParser[T] = PartialFunction[Message, T]

  /**
    * Parses a text message to a string.
    */
  val textMessage2string: MessageParser[String] = {
    case msg: TextMessage => msg.getText
  }
}
