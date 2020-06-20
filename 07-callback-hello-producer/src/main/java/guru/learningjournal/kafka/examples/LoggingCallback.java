package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;

public class LoggingCallback implements Callback {
    private static final Logger logger = LogManager.getLogger();
    private String message;

    public LoggingCallback(String message){
        this.message=message;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e !=null){
            logger.error("Error sending message string = " + message);
        }else {
            logger.info(message + " persisted with offset " + recordMetadata.offset()
                + " and timestamp on " + new Timestamp(recordMetadata.timestamp()));
        }
    }
}
