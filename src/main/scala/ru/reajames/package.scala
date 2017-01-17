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
    override def toString: String = "Queue(%s)" format name
  }

  /**
    * Creates a topic.
    * @param name specifies name of a topic
    */
  case class Topic(name: String) extends DestinationFactory {
    def apply(session: Session): Destination = session.createTopic(name)
    override def toString: String = "Topic(%s)" format name
  }

  /**
    * Creates a temporary unnamed queue.
    */
  case object TemporaryQueue extends DestinationFactory {
    def apply(session: Session): Destination = session.createTemporaryQueue()
    override def toString: String = "TemporaryQueue"
  }

  /**
    * Creates a temporary unnamed topic.
    */
  case object TemporaryTopic extends DestinationFactory {
    def apply(session: Session): Destination = session.createTemporaryTopic()
    override def toString: String = "TemporaryTopic"
  }

  /**
    * Creates a text message using session and a string.
    */
  val string2textMessage: (Session, String) => Message = (session, text) => session.createTextMessage(text)

  /**
    * Creates a message using specified data element as well as message destination.
    * @tparam T specifies data type
    */
  type DestinationAwareMessageFactory[T] = (Session, T) => (Message, Destination)

  def permanentDestination[T](destinationFactory: DestinationFactory)(messageFactory: (Session, T) => Message): DestinationAwareMessageFactory[T] =
    (session, elem) => (messageFactory(session, elem), destinationFactory(session))

  def replyTo[T](messageFactory: (Session, T) => Message): DestinationAwareMessageFactory[(T, Destination)] =
    (session, elem) => (messageFactory(session, elem._1), elem._2)
}
