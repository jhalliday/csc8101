package uk.ac.ncl.cs.csc8101.weblogcoursework;

import com.codahale.metrics.Meter;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * Thread scoped processing logic that mostly just delegates to the provided MessageHandler
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2015-01
 */
public class RunnableConsumer implements Runnable {

    private final KafkaStream<String, String> stream;
    private final MessageHandler messageHandler;
    private final Meter meter;

    public RunnableConsumer(KafkaStream<String, String> stream, MessageHandler messageHandler, Meter meter) {
        this.stream = stream;
        this.messageHandler = messageHandler;
        this.meter = meter;
    }

    public void run() {

        try {

            ConsumerIterator<String, String> it = stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<String, String> messageAndMetadata = it.next();
                String message = messageAndMetadata.message();
                messageHandler.handle(message);
                meter.mark();
            }
            messageHandler.flush();

        } catch (ConsumerTimeoutException e) {
            messageHandler.flush();
        }
    }
}
