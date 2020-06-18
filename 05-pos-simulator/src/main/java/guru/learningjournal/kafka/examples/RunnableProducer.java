package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.datagenerator.InvoiceGenerator;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class RunnableProducer implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private KafkaProducer<String, PosInvoice> producer;
    private String topicName;
    private int produceSpeed;
    private InvoiceGenerator invoiceGenerator;
    private int id;
    // a boolean variable used to read and write atomically for multiple thread
    private final AtomicBoolean stopper = new AtomicBoolean(false);

    // constructor
    RunnableProducer(int id, KafkaProducer<String, PosInvoice> producer, String topicName, int produceSpeed){
        this.id = id;
        this.topicName = topicName;
        this.produceSpeed = produceSpeed;
        this.producer = producer;
        this.invoiceGenerator = InvoiceGenerator.getInstance();
    }

    @Override
    public void run() {
        try{
            logger.info("Starting producer thread - " + id);
            while(!stopper.get()){
                //get next invoice
                PosInvoice posInvoice = invoiceGenerator.getNextInvoice();
                producer.send(new ProducerRecord<>(topicName, posInvoice.getStoreID(), posInvoice));
                //sleep to keep some time between sending new records
                Thread.sleep(produceSpeed);
            }
        }catch (Exception e){
            logger.error("Exception in producer thread - "+id);
            throw  new RuntimeException(e);
        }
    }
    void shutdown(){
        logger.info("Shutting down producer thread - "+ id);
        stopper.set(true);
    }
}
