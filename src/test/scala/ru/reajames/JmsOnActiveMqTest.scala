package ru.reajames

import org.scalatest.FlatSpec
import org.apache.activemq.ActiveMQConnectionFactory

/**
  * Tests Jms using ActiveMQ as a JMS broker.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 27.12.16 23:10.
  */
class JmsOnActiveMqTest extends FlatSpec with JmsSpec {
  // TODO: It needs to properly shutdown the broker
  val connectionFactory = new ActiveMQConnectionFactory("vm://test-broker?broker.persistent=false&broker.useJmx=false")
}


