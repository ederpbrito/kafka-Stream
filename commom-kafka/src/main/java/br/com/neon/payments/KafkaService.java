package br.com.neon.payments;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService<T> implements Closeable, ConsumerRebalanceListener {

	private KafkaConsumer<String, T> consumer;
	private final ConsumerFunction<T> parse;	

	public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Class<T> type,
			Map<String, String> properties) {
		this(parse, groupId, type, properties);
		consumer.subscribe(Collections.singletonList(topic));
	}

	public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Class<T> type,
			Map<String, String> properties) {
		this(parse, groupId, type, properties);
		consumer.subscribe(topic);
	}

	private KafkaService(ConsumerFunction<T> parse, String groupId, Class<T> type, Map<String, String> properties) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<String, T>(getProperties(groupId, type, properties));
	}

	void run() {

		while (true) {
			var records = consumer.poll(Duration.ofMillis(100));
			
			if (!records.isEmpty()) {
				System.out.println("found " + records.count() + " message!");
				for (var record : records) {
					parse.consume(record);
					consumer.commitAsync();
				}
			}
		}

	}

	private Properties getProperties(String groupId, Class<T> type, Map<String, String> overrideProperties) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "execution_payment");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
		properties.putAll(overrideProperties);
		return properties;
	}

	@Override
	public void close() {
		this.consumer.close();
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {		
		
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {		
		
	}

}
