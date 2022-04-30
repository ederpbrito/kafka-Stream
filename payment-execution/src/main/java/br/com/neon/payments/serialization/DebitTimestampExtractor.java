package br.com.neon.payments.serialization;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DebitTimestampExtractor implements TimestampExtractor {

	final static Logger logger = LoggerFactory.getLogger(DebitTimestampExtractor.class);
	
	@Override
	public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
	   return System.currentTimeMillis();
	}
}
