package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.Notification;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import serde.AppSerdes;

import java.util.Properties;

public class RewardsApp {
    private static final Logger logger =LogManager.getLogger();

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        prop.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        //create Streambuilder object
        StreamsBuilder builder = new StreamsBuilder();
        //create kstream with Prime type customers
        KStream<String, PosInvoice> KS0 = builder.stream(AppConfigs.posTopicName,
                Consumed.with(AppSerdes.String(),AppSerdes.PosInvoice()))
                .filter((key,value) -> value.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME));

        //create kafka store builder object having in memory key-value store configuration
        StoreBuilder kvStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(AppConfigs.REWARDS_STORE_NAME),
                AppSerdes.String(), AppSerdes.Double());
        // add kvStorebuilder to streams builder object using addStateStore method
        builder.addStateStore(kvStoreBuilder);
        //repartition records based on customercard no,
        // posinvoice to notification and send it to notification topic
        KS0.through(AppConfigs.REWARDS_TEMP_TOPIC_NAME, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice()))
                .transformValues(() -> new RewardsTransformer(),AppConfigs.REWARDS_STORE_NAME)
                .to(AppConfigs.notificationTopic, Produced.with(AppSerdes.String(),AppSerdes.Notification()));

        //create kafka stream
        KafkaStreams stream = new KafkaStreams(builder.build(),prop);
        stream.start();
        logger.info("Streaming started");

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Closing stream processing");
            stream.close();
        }));

    }
}
