package guru.learningjournal.kafka.examples;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A sample app to demonstrate KTable used for consuming data from Kafka producer instead of
 * consumer or Kafka streams.
 */

import java.util.Properties;

public class StreamingTableApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        //set properties for stream config
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        prop.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create a StreamBuilder object
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // create a Ktable
        KTable<String, String> KT0 = streamsBuilder.table(AppConfigs.topicName);
        KT0.toStream().print(Printed.<String, String>toSysOut().withLabel("KT0")); //print it

        //filter hdfc & TCS record where values are not empty
        KTable<String, String> KT1 = KT0.filter((k,v) -> k.matches(AppConfigs.regExSymbol) && !v.isEmpty(),
                Materialized.as(AppConfigs.stateStoreName));
        KT1.toStream().print(Printed.<String, String>toSysOut().withLabel("KT1")); //print it

        // create kafka streams
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(),prop);

        //define QueryServer to query Ktable
        QueryServer qserver = new QueryServer(streams, AppConfigs.queryServerHost, AppConfigs.queryServerPort);
        streams.setStateListener((newstate, oldstate) -> {
            logger.info("State changing to "+ newstate + "from " + oldstate);
            qserver.setActive(newstate == KafkaStreams.State.RUNNING && oldstate == KafkaStreams.State.REBALANCING);
        });

        //start streaming
        streams.start();
        //start queryserver
        qserver.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("closing streams");
            streams.close();
            qserver.stop();
        }));
    }

}
