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
    public static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        logger.info("Creating Kafka producer ..");
        Properties prop = new Properties();

        // set producer properties
        prop.put(ProducerConfig.CLIENT_ID_CONFIG,AppConfigs.applicationID); // applicationID to denote msg source
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers); //host,ports for kafka brokers
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class.getName()); // key serializer
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName()); // value serializer

        // create kafka producer instance
        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer, String>(prop);

        logger.info("Start sending messages to broker ..");

        // send messages
        for(int i=0; i< AppConfigs.numEvents;i++)
                producer.send(new ProducerRecord<>(AppConfigs.topicName,i,"Simple message :"+ i));
        logger.info("finished sending messages");
        //close the producer
        producer.close();


    }

}
