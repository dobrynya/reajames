package ru

import javax.jms._

/**
  * Contains helpful types and functions.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 20.12.16 23:55.
  */
package object reajames {

  /**
    * Creates a destination using specified session.
    */
  type DestinationFactory = Session => Destination

  /**
    * Creates a message using specified data element and session.
    * @tparam T specifies data type
    * @tparam M specifies message type
    */
  type MessageFactory[T, M <: Message] = Session => T => M

  /**
    * Creates a text message using session and a string.
    */
  val string2textMessage: MessageFactory[String, TextMessage] = session => text => session.createTextMessage(text)
}
