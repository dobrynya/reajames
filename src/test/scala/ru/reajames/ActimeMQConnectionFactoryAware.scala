package ru.reajames

import org.apache.activemq.broker.BrokerService
import org.apache.activemq.ActiveMQConnectionFactory

/**
  * Provides a connection factory for testing purpose.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 17.01.17 12:58.
  */
trait ActimeMQConnectionFactoryAware {
  import ActiveMQStarter._

  brokerService.getVmConnectorURI

  implicit val connectionFactory = new ActiveMQConnectionFactory("vm://localhost")

  val holder = new ConnectionHolder(connectionFactory)

  (for {
    c <- holder.connection
    s <- Jms.session(c)
  } yield s).foreach(println)

  def failingConnectionFactory = new ActiveMQConnectionFactory("tcp://non-existent-host:61616")
}

object ActiveMQStarter {
  val brokerService = new BrokerService()
  brokerService.setUseJmx(false)
  brokerService.setPersistent(false)
  brokerService.setUseShutdownHook(false)
  brokerService.start()
}
