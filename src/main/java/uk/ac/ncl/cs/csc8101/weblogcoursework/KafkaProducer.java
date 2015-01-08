package uk.ac.ncl.cs.csc8101.weblogcoursework;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * Command line data loading script to read a file into a kafka queue.
 * Life would be easier if kafka-console-producer.sh supported configuration of the partitioner.
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2015-01
 * @see https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 */
public class KafkaProducer {

//    private static final File dataDir = new File("/run/media/jhalli/4d0dafed-400e-48a3-9da1-92ed59d928f5/wc98/decoded/");
    private static final File dataDir = new File("/home/jhalli/IdeaProjects/ncl/");
    // 1,352,794,346 lines; 13,050,324,662 bytes (13G), md5sum=b7089321366fe6f8131196b81d060c5d
    private static final File logFile = new File(dataDir, "all-clean.gz");

    public static void main(String[] args) throws Exception {

        // bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 16 --topic csc8101

        final MetricRegistry metricRegistry = new MetricRegistry();
        final Meter meter = metricRegistry.meter("throughput");
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);

        final Properties properties = new Properties();
        properties.setProperty("metadata.broker.list", "localhost:9092");
        properties.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        properties.setProperty("partitioner.class", "uk.ac.ncl.cs.csc8101.weblogcoursework.LogPartitioner");
        properties.setProperty("request.required.acks", "1");
        properties.setProperty("producer.type", "async");
        properties.setProperty("batch.num.messages", "10000");

        //properties.setProperty("queue.buffering.max.messages", "10000");
        //properties.setProperty("send.buffer.bytes", "1048576");
        //properties.setProperty("compression.codec", "gzip");

        final ProducerConfig producerConfig = new ProducerConfig(properties);
        final Producer<String, String> producer = new Producer<>(producerConfig);
        final String topic = "csc8101x";

        try (
                final FileInputStream fileInputStream = new FileInputStream(logFile);
                final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
                final InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream);
                final BufferedReader bufferedReader = new BufferedReader(inputStreamReader)
        ) {
            String line;
            while((line = bufferedReader.readLine()) != null) {
                final String clientId = line.substring(0, line.indexOf(' '));

                final KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(topic, clientId, line);
                producer.send(keyedMessage);

                meter.mark();
            }
        }

        producer.close();

        reporter.report();
        reporter.stop();

        // bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic csc8101 --time -1 | sed -e "s/.*://" | awk '{s+=$1} END {print s}'
    }
}