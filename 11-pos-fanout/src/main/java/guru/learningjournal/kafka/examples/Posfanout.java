package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Properties;

public class Posfanout {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        //set stream properties
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);

        //create Stream builder
        StreamsBuilder builder = new StreamsBuilder();
        //create KStream object. Here we have also supplied Consumed.with object used internally for consuming with Key
        //& value serdes
        KStream<String, PosInvoice> KS0 = builder.stream(AppConfigs.posTopicName,
                Consumed.with(AppSerdes.String(),AppSerdes.PosInvoice()));
        //problem 1 - filter the home delivery records and send it to shipment topic
        //here also in to , we supply the Produced.with produce object with key value serdes
        KS0.filter((k,v) ->
                v.getDeliveryType().equalsIgnoreCase(AppConfigs.DELIVERY_TYPE_HOME_DELIVERY))
        .       to(AppConfigs.shipmentTopicName, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice()));

        //problem -2 - filter the prime customers and send the notification to Loyalty department
        KS0.filter((k,v) ->
                v.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME))
                .mapValues(invoice -> RecordBuilder.getNotification(invoice))
                .to(AppConfigs.notificationTopic,Produced.with(AppSerdes.String(),AppSerdes.Notification()));

        // problem 3 - mask the  PI data using getMaskedinvoice method and send to hadoop topic for trend analytics
        KS0.mapValues(invoice -> RecordBuilder.getMaskedInvoice(invoice))
                .flatMapValues(maskedInvoice -> RecordBuilder.getHadoopRecords(maskedInvoice))
                .to(AppConfigs.hadoopTopic, Produced.with(AppSerdes.String(), AppSerdes.HadoopRecord()));

        //create kafka stream and start streaming
        KafkaStreams streams = new KafkaStreams(builder.build(),prop);
        logger.info("Starting stream processing");
        streams.start();

        //add shutdonw hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Closing stream processing");
            streams.close();
        }));


    }
}
