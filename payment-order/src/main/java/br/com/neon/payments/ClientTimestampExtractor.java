package br.com.neon.payments;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClientTimestampExtractor implements TimestampExtractor {
	
	final static Logger logger = LoggerFactory.getLogger(ClientTimestampExtractor.class);
	@Override
	public long extract(ConsumerRecord<Object, Object> consumerRecord, long partitionTime) {
		 final long timestamp = consumerRecord.timestamp();

	        if ( timestamp < 0 ) {
	            final String msg = consumerRecord.toString().trim();
	            logger.warn( "Record has wrong Kafka timestamp: {}. It will be patched with local timestamp. Details: {}", timestamp, msg );
	            return System.currentTimeMillis();
	        }

	        return timestamp;
	}

}
