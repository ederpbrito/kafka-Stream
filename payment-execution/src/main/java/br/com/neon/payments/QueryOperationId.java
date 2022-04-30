package br.com.neon.payments;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import br.com.neon.payments.domain.Debit;
import br.com.neon.payments.serialization.DebitSerdes;
import br.com.neon.payments.serialization.DebitTimestampExtractor;

public class QueryOperationId {

	private static KafkaStreams streams;
	private static StringBuffer operationQuery = new StringBuffer();
	private static StringBuffer operationProcessed = new StringBuffer();

	public static void main(String[] args) throws IOException, InterruptedException {
		buildTopologyStreamDebit();
		
		streams.cleanUp();

		streams.start();

		while (streams.state() != KafkaStreams.State.RUNNING) {
			System.out.println("Waiting for kafka execution state!");
			Thread.sleep(1000);
		}
			
		
		Scanner scanner = new Scanner(System.in);  	   
	    
	    boolean conti = true;
	    StringBuilder client = new StringBuilder();
	    while(conti) {
	    	System.out.println("Digite o cliente: ");
	    	client.append(scanner.nextLine());   		
	    	isRecordAlreadyProcessed(client.toString());
	    	client.delete(0, client.length());
	    	
	    	System.out.println("Continue? [S/N]");
	    	client.append(scanner.nextLine());
	    	
	    	conti = client.toString().contains("S");
	    	client.delete(0, client.length());
	    }
		
	    streams.close();
	    
		//Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	private static boolean isRecordAlreadyProcessed(String idClient) {
		ReadOnlyKeyValueStore<String, Debit> keyValueStore = streams
				.store(StoreQueryParameters.fromNameAndType("debit-counts", QueryableStoreTypes.keyValueStore()));
		Debit debitProcessed = keyValueStore.get(idClient);

		if (debitProcessed != null) {
			System.out.println("Operation_uuid já processados para o cliente " + idClient + ": ");
			for (String operation_uuid : debitProcessed.getListProcessed()) {
				System.out.println("Operation_uuid: " + operation_uuid);
			}
		} else {
			System.out.println("Cliente ainda não foi processado!");
		}

		return false;
	}

	private static void buildTopologyStreamDebit() throws IOException {
		StreamsBuilder builder = new StreamsBuilder();

		Consumed<String, Debit> debitConsumed = Consumed.with(Serdes.String(), new DebitSerdes())
				.withTimestampExtractor(new DebitTimestampExtractor());

		KStream<String, Debit> textLines = builder.stream("ORQUESTRATION_NEW_DEBIT", debitConsumed);

		KStream<String, Debit> filter = textLines.filter(new Predicate<String, Debit>() {
			@Override
			public boolean test(String key, Debit value) {
				return value.getUuid_operation().contains(operationQuery);
			}
		});

		KGroupedStream<String, Debit> grouped = textLines.groupByKey();

		@SuppressWarnings("unused")
		KTable<String, Debit> debitCounts = grouped.reduce(new Reducer<Debit>() {
			@Override
			public Debit apply(Debit value1, Debit value2) {

				if (!value1.getListProcessed().contains(value1.getUuid_operation())) {
					value1.listAddProcessed(value1.getUuid_operation());
				}

				if (!value1.getListProcessed().contains(value2.getUuid_operation())) {
					value1.listAddProcessed(value2.getUuid_operation());
				}

				return value1;
			}
		}, Materialized.as("debit-counts"));

		// debitCounts.toStream().print(Printed.<String,
		// Debit>toSysOut().withLabel("debit-counts"));

		filter.foreach((key, value) -> operationProcessed.append(value.getUuid_operation()));

		Topology topology = builder.build();
		streams = new KafkaStreams(topology, getPropertiesDebit());

	}

	private static Properties getPropertiesDebit() {
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "execution_payment");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, DebitSerdes.class);
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
		streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, DebitTimestampExtractor.class);

		Path stateDirectory = null;
		try {
			stateDirectory = Files.createTempDirectory("kafka-streams");
		} catch (IOException e) {
			System.out.println("erro: " + e.getMessage());
		}
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toAbsolutePath().toString());
		return streamsConfiguration;
	}

}
