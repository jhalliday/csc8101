package uk.ac.ncl.cs.csc8101.weblogcoursework;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Command line script to read and process a kafka queue.
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2015-01
 * @see https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example
 */
public class KafkaConsumer {

    public static void main(String[] args) throws Exception {

        final MetricRegistry metricRegistry = new MetricRegistry();
        final Meter meter = metricRegistry.meter("throughput");
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);

        final Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "myclient");
        properties.setProperty("zookeeper.session.timeout.ms", "400");
        properties.setProperty("zookeeper.sync.time.ms", "200");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("auto.offset.reset", "smallest");
        properties.setProperty("consumer.timeout.ms", "10000");

        final ConsumerConfig consumerConfig = new ConsumerConfig(properties);

        final String topic = "csc8101";
        final int numThreads = 4;

        final ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        final Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, numThreads);
        final Decoder<String> decoder = new StringDecoder(new VerifiableProperties());
        final Map<String, List<KafkaStream<String, String>>> streamsMap =
                consumerConnector.createMessageStreams(topicCountMap, decoder, decoder);

        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        for(final KafkaStream<String, String> stream : streamsMap.get(topic)) {
            final MessageHandler messageHandler = new MessageHandler();
            final RunnableConsumer runnableConsumer = new RunnableConsumer(stream, messageHandler, meter);
            executorService.submit(runnableConsumer);
        }

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);

        consumerConnector.shutdown();

        MessageHandler.close();

        reporter.report();
        reporter.stop();

        // bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zkconnect localhost:2181 --group myclient
    }
}
