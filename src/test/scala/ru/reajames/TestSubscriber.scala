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

/**
  * Just sends specified list to a subscriber on request.
  * @param list messages to be sent
  * @tparam T message type
  */
case class QueuePublisher[T](list: List[T]) extends Publisher[T] {
  def subscribe(s: Subscriber[_ >: T]): Unit = {
    s.onSubscribe(new Subscription {
      var queue = list
      var cancelled = Option.empty[Boolean]

      def cancel() = cancelled match {
        case None => cancelled = Some(true)
        case _ =>
      }

      def request(n: Long) =
        cancelled.getOrElse {
          queue.headOption.map { e =>
            s.onNext(e)
            queue = queue.tail
          }.getOrElse(s.onComplete())
        }
    })
  }
}
