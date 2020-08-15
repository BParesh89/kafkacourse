package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.SimpleInvoice;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class InvoiceTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTime) {
        SimpleInvoice invoice = (SimpleInvoice) consumerRecord.value();
        long eventTime = Instant.parse(invoice.getCreatedTime()).toEpochMilli();
        return ((eventTime>0)  ? eventTime : prevTime);
    }
}
