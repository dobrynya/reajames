package ru.reajames

import javax.jms.Message
import org.reactivestreams._

/**
  * Convenient subscriber for testing purpose.
  * @author Dmitry Dobrynin <dobrynya@inbox.ru>
  *         Created at 22.12.16 1:44.
  */
case class TestSubscriber(subscribe: Subscription => Unit = s => (),
                          next: (Subscription, Message) => Unit = (s, msg) => (), complete: () => Unit = () => (),
                          error: Throwable => Unit = th => (),
                          request: Option[Long] = None) extends Subscriber[Message] {

  var subscription: Subscription = _

  /**
    * Forwards error handling to the corresponding function.
    * @param th an error occurred
    */
  def onError(th: Throwable): Unit = error(th)

  /**
    * Forwards handling to the corresponding function.
    * @param s specifies subscription
    */
  def onSubscribe(s: Subscription): Unit = {
    subscription = s
    subscribe(s)
    request.foreach(s.request)
  }

  /**
    * Forwards handling to the corresponding function.
    */
  def onComplete(): Unit = complete()

  /**
    * Forwards handling to the corresponding function.
    * @param msg a received message
    */
  def onNext(msg: Message): Unit = next(subscription, msg)
}
