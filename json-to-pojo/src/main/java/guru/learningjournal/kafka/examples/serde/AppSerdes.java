package guru.learningjournal.kafka.examples.serde;

import guru.learningjournal.kafka.examples.types.DeliveryAddress;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;
// Creating your own serde class by extending Serdes gives you advantage of having built-in serdes
// in your own class as well
public class AppSerdes extends Serdes {

    static public final class PosInvoiceSerde extends WrapperSerde<PosInvoice> {
        public PosInvoiceSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    static public Serde<PosInvoice> PosInvoice() {
        PosInvoiceSerde serde =  new PosInvoiceSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static public final class DeliveryAddressSerde extends WrapperSerde<DeliveryAddress> {
        public DeliveryAddressSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    static public Serde<DeliveryAddress> DeliveryAddress() {
        DeliveryAddressSerde serde =  new DeliveryAddressSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, DeliveryAddress.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }
}
