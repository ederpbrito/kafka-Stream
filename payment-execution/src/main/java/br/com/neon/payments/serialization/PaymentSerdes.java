package br.com.neon.payments.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import br.com.neon.payments.domain.Payment;

public class PaymentSerdes implements Serde<Payment> {

	private JsonSerializer<Payment> serializer = new JsonSerializer<>();
	private JsonDeserializer<Payment> deserializer = new JsonDeserializer<>(Payment.class);
	
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
    public Serializer<Payment> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Payment> deserializer() {
        return deserializer;
    }
}
