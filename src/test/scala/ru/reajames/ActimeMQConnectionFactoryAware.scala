package ru.reajames

import org.apache.activemq.ActiveMQConnectionFactory

/**
  * Provides a connection factory for testing purpose.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 17.01.17 12:58.
  */
trait ActimeMQConnectionFactoryAware {
  implicit val connectionFactory =
    new ActiveMQConnectionFactory("vm://test-broker?broker.persistent=false&broker.useJmx=false")
  def failingConnectionFactory =
    new ActiveMQConnectionFactory("tcp://non-existent-host:61616")
}
