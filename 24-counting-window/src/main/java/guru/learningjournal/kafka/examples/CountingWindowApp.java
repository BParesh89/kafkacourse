package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.SimpleInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Time;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;


public class CountingWindowApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        //set Stream Configuration properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreName);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
        //create StreamBuilder object
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        //create KStream object
        KStream<String, SimpleInvoice> KS0 = streamsBuilder.stream(AppConfigs.posTopicName,
            Consumed.with(AppSerdes.String(), AppSerdes.SimpleInvoice())
                .withTimestampExtractor(new InvoiceTimeExtractor())
        );
        //create KTable with count of invoices every 5 min tumbling window
        KTable<Windowed<String>, Long> KT0 = KS0.groupByKey(Grouped.with(AppSerdes.String(), AppSerdes.SimpleInvoice()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .count();
        //print each value
        KT0.toStream().foreach(
                (wkey, value) -> logger.info(
                        "Store-ID :" + wkey.key() + " WindowID :" + wkey.window().hashCode() +
                                " Window start :" + Instant.ofEpochMilli(wkey.window().start()).atOffset(ZoneOffset.UTC)+
                                " Window end :" + Instant.ofEpochMilli(wkey.window().end()).atOffset(ZoneOffset.UTC) +
                                " Count :" + value
        )
        );

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            streams.close();
        }));

    }
}
