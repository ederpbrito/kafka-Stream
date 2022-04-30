package br.com.neon.payments.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import br.com.neon.payments.DebitProcessed;

public class DebitProcessedSerdes implements Serde<DebitProcessed> {

	private JsonSerializer<DebitProcessed> serializer = new JsonSerializer<>();
	private JsonDeserializer<DebitProcessed> deserializer = new JsonDeserializer<>(DebitProcessed.class);
	
	@Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<DebitProcessed> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<DebitProcessed> deserializer() {
        return deserializer;
    }
}
