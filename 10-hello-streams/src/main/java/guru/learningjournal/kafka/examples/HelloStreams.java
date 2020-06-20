package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloStreams {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        // set streams properties
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //create Streambuilder object
        StreamsBuilder streamsBuilder= new StreamsBuilder();
        //create KStream
        KStream<Integer, String> kStream = streamsBuilder.stream(AppConfigs.topicName);
        //add a process transformation to kstream
        kStream.foreach((k,v) -> System.out.println("Keys "+ k + " Value - "+ v));

        //create topology to bind process in kstream
        Topology topology = streamsBuilder.build();
        //create kafka stream
        KafkaStreams streams = new KafkaStreams(topology,prop);
        logger.info("Stream processing started ....");
        streams.start();

        //shutdown hook to close stream
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Closing stream processing");
            streams.close();
        }));
    }

}
