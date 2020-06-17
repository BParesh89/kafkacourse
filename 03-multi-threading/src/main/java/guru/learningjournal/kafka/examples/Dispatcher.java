package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Dispatcher implements Runnable {

    private static final Logger logger = LogManager.getLogger();
    private String fileLocation;
    private String topicName;
    private KafkaProducer<Integer,String> producer;

    // constructor for producer
    Dispatcher(KafkaProducer<Integer,String> producer, String topicName, String fileLocation){
        this.producer = producer;
        this.topicName = topicName;
        this.fileLocation = fileLocation;
    }
    @Override
    public void run() {
        logger.info("Start processing messages in "+ fileLocation);
        File file = new File(fileLocation);
        int counter = 0; //counter for message count

        try (Scanner scanner = new Scanner(file)){
            while(scanner.hasNextLine()){
                // read nextline
                String line = scanner.nextLine();
                //send line wrapped as producerRecord
                producer.send(new ProducerRecord<>(topicName,null, line));
                counter++;
            }
            logger.info("Finished sending " + counter + " messages from " + fileLocation);
        }
        catch (FileNotFoundException e){
            throw new RuntimeException(e);
        }
    }
}
