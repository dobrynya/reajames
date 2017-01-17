package ru.reajames

import org.scalatest.FlatSpec

/**
  * Tests Jms using ActiveMQ as a JMS broker.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 27.12.16 23:10.
  */
class JmsOnActiveMqTest extends FlatSpec with JmsSpec with ActimeMQConnectionFactoryAware