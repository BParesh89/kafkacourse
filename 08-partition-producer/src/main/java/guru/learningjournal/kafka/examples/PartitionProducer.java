package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.Properties;

public class PartitionProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.trace("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OddEvenPartitioner.class);


        RecordMetadata metadata;
        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                Thread.sleep(1000);
                metadata = producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Simple Message-" + i)).get();
                logger.info("Message " + i + " persisted with offset " + metadata.offset()
                    + " in partition " + metadata.partition());
            }
        } catch (Exception e) {
            logger.info("Can't send message - Received exception \n" + e.getMessage());
        }

    }
}
