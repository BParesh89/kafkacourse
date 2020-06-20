package guru.learningjournal.kafka.examples;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import guru.learningjournal.kafka.examples.serde.JsonDeserializer;
import guru.learningjournal.kafka.examples.serde.JsonSerializer;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class PosValidator {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String[] args) {
        // define consumer config properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfigs.groupID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer instance
        KafkaConsumer<String, PosInvoice> consumer = new KafkaConsumer<String, PosInvoice>(consumerProps);
        // subscribe to a list of topcis - here we have only one
        consumer.subscribe(Arrays.asList(AppConfigs.sourceTopicNames));

        /**
         now we will create a producer to send valid and invalid records on different topics based on
         if deliverytype = 'HOME_DELIVERY' and contact_number is null then it is invalid record
         **/

        // define producer properties
        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        //create Kafka producer
        KafkaProducer<String, PosInvoice> producer = new KafkaProducer<String, PosInvoice>(producerProp);

        /**consume records from consumer and send them to validtopic if they are valid. Invalid records
        must be sent to invalidtopic using producer.
         **/
        logger.info("Starting consumer to consume records");
        while (true){
            // get records via consumer.poll method every 100 ms
            ConsumerRecords<String, PosInvoice> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, PosInvoice> record: records){
                //check for invalid record
                if(record.value().getDeliveryType()=="HOME-DELIVERY" &&
                        record.value().getDeliveryAddress().getContactNumber()==""){
                    producer.send(new ProducerRecord<>(AppConfigs.invalidTopicName,
                            record.value().getStoreID(), record.value()));
                    logger.info("Invalid record with invoice number - " + record.value().getInvoiceNumber());
                }else{
                    producer.send(new ProducerRecord<>(AppConfigs.validTopicName,
                            record.value().getStoreID(), record.value()));
                }
            }
        }


    }
}
