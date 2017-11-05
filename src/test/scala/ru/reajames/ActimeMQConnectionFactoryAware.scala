package ru.reajames

import org.apache.activemq.broker.BrokerService
import org.apache.activemq.ActiveMQConnectionFactory

/**
  * Provides a connection factory for testing purpose.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 17.01.17 12:58.
  */
trait ActimeMQConnectionFactoryAware {
  val brokerService = new BrokerService()
  brokerService.setUseJmx(false)
  brokerService.setPersistent(false)
  brokerService.setUseShutdownHook(false)
  brokerService.start()

  implicit val connectionFactory = new ActiveMQConnectionFactory("vm://localhost")

  def failingConnectionFactory = new ActiveMQConnectionFactory("tcp://non-existent-host:61616")
}
