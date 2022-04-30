package br.com.neon.payments.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import br.com.neon.payments.domain.Client;

public class ClientSerdes  implements Serde<Client> {

	private JsonSerializer<Client> serializer = new JsonSerializer<>();
	private JsonDeserializer<Client> deserializer = new JsonDeserializer<>(Client.class);
	
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
    public Serializer<Client> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Client> deserializer() {
        return deserializer;
    }

}
