package ru.reajames

import org.scalatest._

/**
  * Specification on ConnectionHolder.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 29.01.17 1:22.
  */
class ConnectionHolderSpec extends FlatSpec with Matchers with ActimeMQConnectionFactoryAware {
  "Connection holder" should "create connection using supplied ConnectionFactory" in {
    val holder = new ConnectionHolder(connectionFactory, Some("TestConnection"), Some("sa" -> ""), Some("new-client-id"))
    holder.connection.isSuccess should equal(true)
  }

  it should "create connection using provided connection constructor" in {
    val holder = new ConnectionHolder(connectionFactory.createConnection(), None)
    holder.connection.isSuccess should equal(true)
  }

  it should "release connection" in {
    new ConnectionHolder(connectionFactory.createConnection(), None).release()
  }

  it should "log if failed" in {
    new ConnectionHolder(failingConnectionFactory).connection.isFailure should equal (true)
  }
}
