ReaJaMeS is JMS in terms of [Reactive Streams](http://www.reactive-streams.org)
====
[![Travis](https://travis-ci.org/dobrynya/reajames.svg?branch=master)](https://travis-ci.org/dobrynya/reajames)

The project is aimed to implementing a JMS client in terms of the Reactive Streams. Anyone can use the library for integrating JMS transport as non-blocking reactive streams identical to other RS implementations.

To subscribe a listener for a queue

```scala
import javax.jms._
import ru.reajames._
import concurrent.ExecutionContext.Implicits.global

def connectionFactory: ConnectionFactory

new JmsReceiver(new ConnectionHolder(connectionFactory), Queue("in-queue"))
  .subscribe(new OnNextSubscriber(message => println(message)))
```

To send messages to a topic
```scala
import javax.jms._
import ru.reajames._
import concurrent.ExecutionContext.Implicits.global

def connectionFactory: ConnectionFactory

def publisher: Publisher[Data]

publisher.subscribe(
  new JmsSender(new ConnectionHolder(connectionFactory), permanentDestination(Topic("events"))(string2textMessage))
)
```