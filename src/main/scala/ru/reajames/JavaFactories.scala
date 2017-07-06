package ru.reajames

import java.util.function.BiFunction
import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import javax.jms.{ConnectionFactory, Message, Session, TextMessage, Destination => JmsDest}

/**
  * Provides methods to create objects.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 15.02.17 23:40.
  */
object JavaFactories {

  /**
    * Creates a new connection holder using provided connection factory.
    * @param connectionFactory connection factory to create connection by
    * @return a newly created connection holder
    */
  def connectionHolder(connectionFactory: ConnectionFactory): ConnectionHolder = new ConnectionHolder(connectionFactory)

  /**
    * Creates a new connection holder using provided connection factory.
    * @param name specifies name of a connection to be logged
    * @param connectionFactory connection factory to create connection by
    * @return a newly created connection holder
    */
  def connectionHolder(name: String, connectionFactory: ConnectionFactory) =
    new ConnectionHolder(connectionFactory, Some(name))

  /**
    * Creates a new connection holder using provided connection factory and user credentials.
    * @param connectionFactory connection factory to create connection by
    * @param user specifies a user to create connection
    * @param password specifies a password to create connection
    * @return a newly created connection holder
    */
  def connectionHolder(connectionFactory: ConnectionFactory, user: String, password: String): ConnectionHolder =
    new ConnectionHolder(connectionFactory, None, Some(user -> password))

  /**
    * Creates a new connection holder using provided connection factory and user credentials.
    * @param name specifies name of a connection to be logged
    * @param connectionFactory connection factory to create a connection by
    * @param user specifies a user to create connection
    * @param password specifies a password to create connection
    * @return a newly created connection holder
    */
  def connectionHolder(name: String, connectionFactory: ConnectionFactory, user: String, password: String): ConnectionHolder =
    new ConnectionHolder(connectionFactory, Some(name), Some(user -> password))

  /**
    * Creates a new connection holder using provided connection factory and user credentials.
    * @param connectionFactory connection factory to create connection by
    * @param clientId specifies client identifier to assign created connection
    * @param user specifies a user to create connection
    * @param password specifies a password to create connection
    * @return a newly created connection holder
    */
  def connectionHolder(connectionFactory: ConnectionFactory, clientId: String, user: String, password: String): ConnectionHolder =
    new ConnectionHolder(connectionFactory, None, Some(user -> password), Some(clientId))

  /**
    * Creates a new connection holder using provided connection factory and user credentials.
    * @param name specifies name of a connection to be logged
    * @param connectionFactory connection factory to create connection by
    * @param clientId specifies client identifier to assign created connection
    * @param user specifies a user to create connection
    * @param password specifies a password to create connection
    * @return a newly created connection holder
    */
  def connectionHolder(name: String, connectionFactory: ConnectionFactory, clientId: String, user: String, password: String): ConnectionHolder =
    new ConnectionHolder(connectionFactory, Some(name), Some(user -> password), Some(clientId))

  /**
    * Creates a queue.
    * @param name specifies name of a queue
    * @return a newly created queue
    */
  def createQueue(name: String): DestinationFactory = Queue(name)

  /**
    * Creates a topic.
    * @param name specifies name of a queue
    * @return a newly created topic
    */
  def createTopic(name: String): DestinationFactory = Topic(name)

  /**
    * Creates a temporary queue.
    * @return a newly created temporary queue
    */
  def createTemporaryQueue(): DestinationFactory = TemporaryQueue()

  /**
    * Creates a temporary topic.
    * @return a newly created temporary topic
    */
  def createTemporaryTopic(): DestinationFactory = TemporaryTopic()

  /**
    * Creates a pair of elements.
    * @param a the first element
    * @param b the second element
    * @tparam A the first element's type
    * @tparam B the second's element type
    * @return
    */
  def pair[A, B](a: A, b: B): (A, B) = (a, b)

  def textMessage(session: Session, text: String): TextMessage = session.createTextMessage(text)

  /**
    * Creates a JMS receiver.
    * @param connectionHolder specifies connection holder
    * @param destinationFactory specifies destination factory
    * @param executor executor being used to receive messages
    * @return a newly created receiver
    */
  def createReceiver(connectionHolder: ConnectionHolder, destinationFactory: DestinationFactory, executor: Executor): JmsReceiver =
    new JmsReceiver(connectionHolder, destinationFactory)(ExecutionContext.fromExecutor(executor))

  /**
    * Creates a JMS sender.
    * @param connectionHolder pecifies connection holder
    * @param destinationFactory specifies destination to send messages to
    * @param messageFactory specifies message factory to create messages
    * @param executor executor being used to send messages
    * @tparam T element type
    * @return a newly created sender
    */
  def createSender[T](connectionHolder: ConnectionHolder, destinationFactory: DestinationFactory,
                      messageFactory: BiFunction[Session, T, Message], executor: Executor): JmsSender[T] =
    new JmsSender[T](connectionHolder, destinationFactory,
      (session, elem) => messageFactory.apply(session, elem))(ExecutionContext.fromExecutor(executor))

  /**
    * Creates a JMS sender.
    * @param connectionHolder pecifies connection holder
    * @param messageFactory specifies message factory to create messages and destination
    * @param executor executor being used to send messages
    * @tparam T element type
    * @return a newly created sender
    */
  def createSender[T](connectionHolder: ConnectionHolder,
                      messageFactory: BiFunction[Session, T, (Message, JmsDest)], executor: Executor): JmsSender[T] =
    new JmsSender[T](connectionHolder, (session, elem) => messageFactory.apply(session, elem))(ExecutionContext.fromExecutor(executor))
}
