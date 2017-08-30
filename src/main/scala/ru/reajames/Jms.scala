package ru.reajames

import scala.util.Try
import javax.jms.{Destination => JmsDestination, _}

/**
  * Provides helpful methods to work with JMS.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 19.12.16 23:24.
  */
object Jms {
  /**
    * Starts the specified connection to be able to deliver messages
    * @param connection specifies connection to start
    * @return success or failure
    */
  def start(connection: Connection): Try[Unit] = Try(connection.start())

  /**
    * Stops the specified connection, so messages cannot be delivered any more.
    * @param connection specifies connection to be stopped
    * @return success or failure
    */
  def stop(connection: Connection): Try[Unit] = Try(connection.stop())

  /**
    * Closes the specified connection.
    * @param connection specifies connection to be closed
    * @return success or failure
    */
  def close(connection: Connection): Try[Unit] = Try(connection.close())

  /**
    * Closes message consumer to stop delivering messages.
    * @param messageConsumer specifies message consumer to be closed
    * @return success or failure
    */
  def close(messageConsumer: MessageConsumer): Try[Unit] = Try(messageConsumer.close())

  /**
    * Closes a session.
    * @param session specifies session to be closed
    * @return success or failure
    */
  def close(session: Session): Try[Unit] = Try(session.close())

  /**
    * Closes a message producer to stop sending messages.
    * @param producer producer to be closed
    * @return success or failure
    */
  def close(producer: MessageProducer): Try[Unit] = Try(producer.close())

  /**
    * Creates a session by the specified connection.
    * @param connection specifies connection to create a session
    * @param acknowledgeMode specifies acknowledge mode or if the session is transaction aware
    * @return created session or failure
    */
  def session(connection: Connection, acknowledgeMode: Int = Session.AUTO_ACKNOWLEDGE): Try[Session] =
    Try(connection.createSession(acknowledgeMode == Session.SESSION_TRANSACTED, acknowledgeMode))

  /**
    * Creates a destination by the specified destination factory.
    * @param session specifies the session to supply to destination factory
    * @param destination specifies destination factory to create destination
    * @return created destination or failure
    */
  def destination(session: Session, destination: DestinationFactory): Try[JmsDestination] =
    Try(destination(session))

  /**
    * Creates a message consumer by the specified session.
    * @param session specifies the session to create consumer
    * @param destination specifies destination of delivered messages
    * @return created consumer or failure
    */
  def consumer(session: Session, destination: JmsDestination): Try[MessageConsumer] =
    Try(session.createConsumer(destination))

  /**
    * Creates a message producer by the specified session.
    * @param session specifies the session to create producer
    * @param destination specifies destination for messages
    * @return created producer or failure
    */
  def producer(session: Session, destination: JmsDestination): Try[MessageProducer] =
    Try(session.createProducer(destination))

  /**
    * Creates a message producer by the specified session.
    * @param session specifies the session to create producer
    * @return created producer which has no specific destination or failure
    */
  @inline
  def producer(session: Session): Try[MessageProducer] = producer(session, null)

  /**
    * Receives an arrived message.
    * @param consumer message consumer
    * @param timeout timeout to wait for arriving
    * @return message or none in case when time out has been appeared or consumer has been closed
    */
  def receive(consumer: MessageConsumer, timeout: Option[Long] = None) =
    Try(Option(timeout.map(consumer.receive).getOrElse(consumer.receive())))

  /**
    * Sends a message with the specified producer.
    * @param producer specifies producer to send a message
    * @param message specifies message to be sent
    * @return sent message or failure
    */
  def send(producer: MessageProducer, message: Message): Try[Message] = Try {
    producer.send(message)
    message
  }

  /**
    * Sends a message with the specified producer to the specified destination.
    * @param producer specifies producer to send a message
    * @param message specifies message to be sent
    * @param destination specifies destination
    * @return sent message or failure
    */
  def send(producer: MessageProducer, message: Message, destination: JmsDestination): Try[Message] = Try {
    producer.send(destination, message)
    message
  }
}
