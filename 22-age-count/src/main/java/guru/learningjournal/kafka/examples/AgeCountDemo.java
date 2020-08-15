package guru.learningjournal.kafka.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class AgeCountDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        //set Stream config properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,100);
        //create StreamBuilder object
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        //create a Ktable with age count of persons and print it
        streamsBuilder.table(AppConfigs.topicName,
            Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((person, age) -> KeyValue.pair(age, "1"), Grouped.with(Serdes.String(), Serdes.String()))
                .count().toStream().print(Printed.<String, Long>toSysOut().withLabel("Age Count"));

        //create kafka stream
        KafkaStreams myStream = new KafkaStreams(streamsBuilder.build(), props);
        //start streaming
        myStream.start();
        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Stream");
            myStream.close();
        }));
    }
}
