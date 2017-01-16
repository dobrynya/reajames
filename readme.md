ReaJaMeS is JMS in terms of [Reactive Streams](http://www.reactive-streams.org)
====
The project is aimed to implementing a JMS client in terms of the Reactive Streams. Anyone can use the library for integrating JMS transport as non-blocking reactive streams identical to other RS implementations.

To subscriber a listener for a queue

```scala
import javax.jms._
import ru.reajames._
import concurrent.ExecutionContext.Implicits.global

def connectionFactory: ConnectionFactory

new JmsReceiver(connectionFactory, Queue("in-queue")).subscribe(new OnNextSubscriber(message => println(message)))
```

To send messages to a topic
```scala
import javax.jms._
import ru.reajames._
import concurrent.ExecutionContext.Implicits.global

def connectionFactory: ConnectionFactory

def publisher: Publisher[Data]

publisher.subscribe(
  new JmsSender[String](connectionFactory, Topic("events"), session => message => session.createTextMessage(element))
)
```