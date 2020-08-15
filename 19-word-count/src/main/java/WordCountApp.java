import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Array;
import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    private static final Logger logger = LogManager.getLogger();
    public static void main(final String[] args) {
        // define stream configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);

        //create StreamBuilder object
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        //create KStream
        KStream<String, String> KS0 = streamsBuilder.stream(AppConfigs.topicName);
        //convert to stream of words
        KStream<String, String> KS1 = KS0.flatMapValues(v -> Arrays.asList(v.toLowerCase().split(" ")));

        //perform groupby based on word
        KGroupedStream<String, String> KGS1 = KS1.groupBy((k,v) -> v);
        //store the word count in KTable
        KTable<String, Long> KT1 = KGS1.count();
        //print KTable values
        KT1.toStream().print(Printed.<String,Long>toSysOut().withLabel("KT1"));

        // create kafka streams
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(),props);

        //start streaming
        streams.start();
        logger.info("Streaming started");

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Closing streams");
            streams.close();
        }));

    }
}
