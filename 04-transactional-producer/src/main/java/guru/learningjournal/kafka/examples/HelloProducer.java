package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfigs.transaction_id); // add transactionid config

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        //initTransactions() ensures any other transactions made by previous producer instances are rolled off
        // or cleaned. It also retrieves internal producer id which is used for idempotence.
        producer.initTransactions();

        //scenario for successful receive
        logger.info("Start first transactions...");
        producer.beginTransaction();
        try{
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Simple Message-T1-" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-T1-" + i));
            }
            logger.info("Commiting first transaction..");
            producer.commitTransaction();
        }catch (Exception e){
            logger.error("Error in first trasnaction. Aborting..");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }
        // rolloff scenario where no message is received by consumer
        logger.info("Start second transactions...");
        producer.beginTransaction();
        try{
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Simple Message-T2-" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-T2-" + i));
            }
            logger.info("Commiting second transaction..");
            producer.abortTransaction();
        }catch (Exception e){
            logger.error("Error in second trasnaction. Aborting..");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }
        logger.info("Finished - Closing Kafka Producer.");
        producer.close();

    }
}
