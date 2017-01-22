package ru.reajames

import org.scalatest._
import java.util.Properties
import collection.JavaConverters._
import java.util.{Hashtable => JHT}
import net.timewalker.ffmq4.utils.Settings
import net.timewalker.ffmq4.local.FFMQEngine
import net.timewalker.ffmq4.jndi.FFMQConnectionFactory

/**
  * Tests JmsSpec using Ffmq as a JMS broker.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 27.12.16 23:22.
  */
class JmsOnFfmqTest extends FlatSpec with JmsSpec with BeforeAndAfterAll {
  val connectionFactory =
    new FFMQConnectionFactory(new JHT[String, AnyRef](Map("java.naming.provider.url" -> "vm://test-broker").asJava))

  def failingConnectionFactory =
    new FFMQConnectionFactory(new JHT[String, AnyRef](Map("java.naming.provider.url" -> "failing://failing-broker").asJava))

  private val broker = {
    val externalProperties = new Properties()
    externalProperties.load(getClass.getResourceAsStream("/ffmq/ffmq-server.properties"))
    val engine = new FFMQEngine("test-broker", new Settings(externalProperties))
    engine.deploy()
    engine
  }

  override protected def afterAll(): Unit = broker.undeploy()
}
