package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Properties;

public class DispatcherDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        try {
            //read kafka bootstrap server location from kafka.properties file
            InputStream inputStream = new FileInputStream(AppConfigs.kafkaConfigFileLocation);
            props.load(inputStream);
            // set kafka producer properties
            props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        } catch (IOException e){
            throw new RuntimeException(e);
        }
        // create producer instance
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        Thread[] dispatchers = new Thread[AppConfigs.eventFiles.length];
        logger.info("Staring Dispatcher threads ...");
        for(int i = 0; i < AppConfigs.eventFiles.length; i++){
            // create new thread and start it
            dispatchers[i] = new Thread(new Dispatcher(producer, AppConfigs.topicName, AppConfigs.eventFiles[i]));
            dispatchers[i].start();
        }
        //join thread handles together
        try{
            for(Thread t: dispatchers) t.join();

        }catch(InterruptedException e){
            logger.error("Main thread interrupted");
        }finally{
            producer.close();
            logger.info("Finished Dispatcher Demo");
        }


    }
}

