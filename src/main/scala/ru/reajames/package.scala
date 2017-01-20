package ru

import java.util.concurrent.atomic.AtomicReference
import javax.jms.{Message, Session, Destination => JmsDestination}

/**
  * Contains helpful types and functions.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 20.12.16 23:55.
  */
package object reajames {

  /**
    * Creates a destination using specified session.
    */
  type DestinationFactory = Session => JmsDestination

  /**
    * Creates a queue.
    * @param name specifies name of a queue
    */
  case class Queue(name: String) extends DestinationFactory {
    def apply(session: Session): JmsDestination = session.createQueue(name)
    override def toString: String = "Queue(%s)" format name
  }

  /**
    * Creates a topic.
    * @param name specifies name of a topic
    */
  case class Topic(name: String) extends DestinationFactory {
    def apply(session: Session): JmsDestination = session.createTopic(name)
    override def toString: String = "Topic(%s)" format name
  }

  /**
    * Contains somehow created destination.
    * @param destination destination to be returned
    */
  case class Destination(destination: JmsDestination) extends DestinationFactory {
    def apply(session: Session): JmsDestination = destination
    override def toString(): String = destination.toString
  }

  /**
    * Creates a destination and caches it for future use. This component should not be used concurrently.
    */
  trait CachingDestinationFactory extends DestinationFactory {
    private var cached = new AtomicReference[JmsDestination]()

    private[reajames] def create(session: Session): JmsDestination

    def apply(session: Session): JmsDestination = {
      if (cached.get() == null) cached.compareAndSet(null, create(session))
      cached.get()
    }

    override def toString(): String =
      if (cached != null) cached.toString else "CachingDestinationFactory"
  }

  /**
    * Creates a text message using session and a string.
    */
  val string2textMessage: (Session, String) => Message = (session, text) => session.createTextMessage(text)

  /**
    * Creates a message using specified data element as well as message destination.
    * @tparam T specifies data type
    */
  type DestinationAwareMessageFactory[T] = (Session, T) => (Message, JmsDestination)

  def permanentDestination[T](destinationFactory: DestinationFactory)(messageFactory: (Session, T) => Message): DestinationAwareMessageFactory[T] =
    (session, elem) => (messageFactory(session, elem), destinationFactory(session))

  def replyTo[T](messageFactory: (Session, T) => Message): DestinationAwareMessageFactory[(T, JmsDestination)] =
    (session, elem) => (messageFactory(session, elem._1), elem._2)
}
