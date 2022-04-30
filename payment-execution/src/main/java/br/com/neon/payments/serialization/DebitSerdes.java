package br.com.neon.payments.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import br.com.neon.payments.domain.Debit;

public class DebitSerdes implements Serde<Debit>{
	
	private JsonSerializer<Debit> serializer = new JsonSerializer<>();
	private JsonDeserializer<Debit> deserializer = new JsonDeserializer<>(Debit.class);
	
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
    public Serializer<Debit> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Debit> deserializer() {
        return deserializer;
    }
}
