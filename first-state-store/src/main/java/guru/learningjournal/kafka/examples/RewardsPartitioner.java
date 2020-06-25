package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Class to implement custom Stream partitioner based on CustomerCard number and number of partitions
 */
public class RewardsPartitioner implements StreamPartitioner<String, PosInvoice> {
    @Override
    public Integer partition(String topic, String key, PosInvoice value, int numPartitions) {
        int i = Math.abs(value.getCustomerCardNo().hashCode()) % numPartitions;
        return i;
    }
}
