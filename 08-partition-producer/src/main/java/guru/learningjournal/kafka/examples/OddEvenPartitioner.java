package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.record.InvalidRecordException;

import java.util.Map;

public class OddEvenPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if ((keyBytes == null) || (!(key instanceof Integer)))
            throw new InvalidRecordException("Topic Key must have a valid Integer value.");

        if (cluster.partitionsForTopic(topic).size() != 2)
            throw new InvalidTopicException("Topic must have exactly two partitions");

        return (Integer) key % 2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
