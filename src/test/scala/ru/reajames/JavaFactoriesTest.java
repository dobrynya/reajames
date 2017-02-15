package ru.reajames;

import org.reactivestreams.*;
import org.testng.annotations.*;
import scala.Function1;
import scala.Option;
import scala.Some;
import javax.jms.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import org.scalatest.testng.TestNGSuite;
import org.apache.activemq.ActiveMQConnectionFactory;
import static java.util.Arrays.asList;
import static org.testng.Assert.*;
import static ru.reajames.JavaFactories.*;
import static ru.reajames.Jms.*;

/**
 * Tests JavaFactories.
 * @author Dmitry Dobrynin <dobrynya@inbox.ru>
 *         Created at 16.02.17 0:28.
 */
public class JavaFactoriesTest extends TestNGSuite {
    ConnectionFactory connectionFactory =
            new ActiveMQConnectionFactory("vm://test-broker?broker.persistent=false&broker.useJmx=false");

    ConnectionHolder connectionHolder = connectionHolder(connectionFactory);

    ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void testCreateReceiver() throws Exception {
        Function1<Session, Destination> q = createQueue("java-dest-1");

        JmsReceiver receiver = createReceiver(connectionHolder, q, executor);

        CountDownLatch latch = new CountDownLatch(5);
        List<String> received = new ArrayList<>();
        receiver.subscribe(new Subscriber<Message>() {
            public void onSubscribe(Subscription s) {
                s.request(5);
            }

            public void onNext(Message message) {
                try {
                    received.add(((TextMessage) message).getText());
                    latch.countDown();
                } catch (JMSException ignored) {}
            }

            public void onError(Throwable th) {
                th.printStackTrace();
            }

            public void onComplete() {}
        });

        Connection c = connectionHolder.connection().get();
        Session s = session(c, false, 1).get();
        MessageProducer p = producer(s, q.apply(s)).get();
        for (int i = 1; i <= 5; i++)
            send(p, textMessage(s, "" + i));

        latch.await(100, TimeUnit.MILLISECONDS);

        assertEquals(asList("1", "2", "3", "4", "5"), asList(received.toArray()));
    }

    @Test
    public void testCreateSender() throws JMSException {
        Function1<Session, Destination> q = createTopic("java-topic-1");
        JmsSender<String> sender = createSender(connectionHolder, q, JavaFactories::textMessage, executor);

        Connection c = connectionHolder.connection().get();
        Session s = session(c, false, 1).get();
        MessageConsumer cons = consumer(s, q.apply(s)).get();

        new Publisher<String>() {
            public void subscribe(Subscriber<? super String> s) {
                s.onSubscribe(new Subscription() {
                    Iterator<String> iter = asList("1", "2", "3", "4", "5").iterator();

                    public void request(long n) {
                        for (int i = 1; i <= n; i++)
                            if (iter.hasNext()) s.onNext("" + iter.next());

                        try {
                            Thread.sleep(100);
                            if (!iter.hasNext()) s.onComplete();
                        } catch (InterruptedException e) {
                            fail("Interrupted!", e, null);
                        }
                    }
                    public void cancel() {}
                });
            }
        }.subscribe(sender);

        for (int i = 1; i <= 5; i++) {
            Option<Message> msg = receive(cons, new Some<>(300L)).get();
            assertEquals("" + i, ((TextMessage) msg.get()).getText());
        }


    }

    @AfterTest
    public void shutdown() {
        executor.shutdown();
    }
}