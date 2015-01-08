package uk.ac.ncl.cs.csc8101.weblogcoursework;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Assigns messages to partitions based on hashing of the key i.e. pins keys consistently to partitions.
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2015-01
 */
public class LogPartitioner implements Partitioner {

    public LogPartitioner(VerifiableProperties verifiableProperties) {}

    public int partition(Object key, int numPartitions) {
        int partition = key.hashCode()%numPartitions;
        if(partition < 0) {
            partition = partition*-1;
        }
        return partition;
    }
}
