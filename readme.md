ReaJaMeS is JMS in terms of [Reactive Streams](http://www.reactive-streams.org)
====
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/2c3f1984c1f4445b8034dd588f1d8b49)](https://www.codacy.com/app/dobrynya/reajames?utm_source=github.com&utm_medium=referral&utm_content=dobrynya/reajames&utm_campaign=badger)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/2c3f1984c1f4445b8034dd588f1d8b49)](https://www.codacy.com/app/dobrynya/reajames?utm_source=github.com&utm_medium=referral&utm_content=dobrynya/reajames&utm_campaign=Badge_Coverage)
[![Travis](https://travis-ci.org/dobrynya/reajames.svg?branch=master)](https://travis-ci.org/dobrynya/reajames)

The project is aimed to implementing a JMS client in terms of the Reactive Streams. Anyone can use the library for integrating JMS transport as non-blocking reactive streams identical to other RS implementations.

More details in [wiki](https://github.com/dobrynya/reajames/wiki). Ideas, improvements are welcome as well as crytical analysis. I will be really appreciated.

To subscribe a listener for a queue

```scala
import ru.reajames._
import javax.jms.ConnectionFactory
import concurrent.ExecutionContext.Implicits.global

def connectionFactory: ConnectionFactory

new JmsReceiver(new ConnectionHolder(connectionFactory), Queue("in-queue"))
  .subscribe(new OnNextSubscriber(message => println(message)))
```

To send text messages to a topic
```scala
import ru.reajames._
import javax.jms.ConnectionFactory
import concurrent.ExecutionContext.Implicits.global

def connectionFactory: ConnectionFactory

def publisher: Publisher[String]

publisher.subscribe(
  new JmsSender(new ConnectionHolder(connectionFactory), Topic("events"), string2textMessage)
)
```