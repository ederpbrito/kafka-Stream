package br.com.neon.payments;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import br.com.neon.payments.domain.Client;
import br.com.neon.payments.domain.ClientWithDebit;
import br.com.neon.payments.domain.Debit;
import br.com.neon.payments.domain.Operation;
import br.com.neon.payments.domain.Payment;
import br.com.neon.payments.dto.ProcessReturn;
import br.com.neon.payments.serialization.ClientSerdes;
import br.com.neon.payments.serialization.DebitProcessedSerdes;
import br.com.neon.payments.serialization.DebitSerdes;
import br.com.neon.payments.serialization.DebitTimestampExtractor;

public class ExecutionClientService {
	
	private static KafkaStreams streamsClient;
	
	private static StringBuffer operationQuery = new StringBuffer();
	private static List<Debit> queryOperations = new ArrayList<>();	
	private static StringBuffer clientQuery = new StringBuffer();
	private static KStream<String, Client> paymentStream;
	private static KStream<String, Debit> debitEvents;
	private static Integer indice = 0;
	private static boolean isKafkaStream = false;

	public static void main(String[] args) throws IOException, InterruptedException {
		var ExecutionService = new ExecutionClientService();
		
		isKafkaStream = args[0].equals("1");
		
		buildTopologyStreamDebit();
		
		streamsClient.cleanUp();
		
		streamsClient.start();
		
		while (streamsClient.state() != KafkaStreams.State.RUNNING) {
			System.out.println("Waiting for kafka execution state!");
			Thread.sleep(1000);
		}	
		
		if(!isKafkaStream) {
			Map<String, String> propCustom = new HashMap<>();
			propCustom.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");						
			
			try (var service = new KafkaService<Client>(ExecutionClientService.class.getSimpleName(),
					"ORQUESTRATION_NEW_CLIENT", record -> {
						ExecutionService.parse(record);
					}, Client.class, propCustom)) {
				service.run();
			}
		}
		
		
		Runtime.getRuntime().addShutdownHook(new Thread(streamsClient::close));
	}

	private static void executeStream() {		
		paymentStream.foreach((key, value) ->{			
			processStreamWithCheck(value.getIdClient(), value.getPayments());
		});
		
	}

	private void parse(ConsumerRecord<String, Client> record) {

		long startTime = System.nanoTime();

		System.out.println("####################################################################");
		System.out.println("Orquestration payment");
		System.out.println("Key: " + record.key());
		System.out.println("Value: " + record.value());
		System.out.println("Partition: " + record.partition());
		System.out.println("Offset: " + record.offset());		

		String idClient = record.value().getIdClient();
		StringBuilder operation_uuid = new StringBuilder();			
		
		processWithCheck(record, record.value().getIdClient(), operation_uuid);
		
		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;

		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		System.out.println("Execution time in nanoseconds: " + timeElapsed);
		System.out.println("Execution time in milliseconds: " + timeElapsed / 1000000);

		System.out.println("Cliente: " + record.value().getIdClient() + " processado!");
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

	}
	
	private static List<Debit> processStreamWithCheck(String idClient, List<Payment> listPayment) {
		for (int count = 0; count < listPayment.size(); count++) {
			System.out.println("*****************************************************************");
			System.out.println("Processando produto: " + listPayment.get(count).getNameProduct());							
			
			if (!isRecordAlreadyProcessed(idClient, listPayment.get(count).getUuid_operation())) {

				// Limpa as variaveis para processamento
				cleanValues();

				// Adiciona operationa_uuid para consulta
				operationQuery.append(listPayment.get(count).getUuid_operation());

				// Chama a api de command do produto
				commandPaymentProduct(idClient, listPayment.get(count));

				System.out.println("Aguardando retorno conta corrente");
				// Escuta o evento de déibto automatico
				// getEventProduct(idClient, operation_uuid.toString());
				getEventProduct(idClient);
			}
		}
		
		return queryOperations;
	}

	private void processWithCheck(ConsumerRecord<String, Client> record, String idClient, StringBuilder operation_uuid) {
		for (int count = 0; count < record.value().getPayments().size(); count++) {
			System.out.println("*****************************************************************");
			System.out.println("Processando produto: " + record.value().getPayments().get(count).getNameProduct());

			operation_uuid.append(record.value().getPayments().get(count).getUuid_operation());				
			
			if (!isRecordAlreadyProcessed(idClient, operation_uuid.toString())) {

				// Limpa as variaveis para processamento
				cleanValues();
				operation_uuid.delete(0, operation_uuid.length());

				// Adiciona operationa_uuid para consulta
				clientQuery.append(record.key());

				// Chama a api de command do produto
				commandPaymentProduct(record.key(), record.value().getPayments().get(count));

				//System.out.println("Aguardando retorno conta corrente");
				// Escuta o evento de déibto automatico
				// getEventProduct(idClient, operation_uuid.toString());
				
				getEventProduct(record.key());
			}
		}
	}
	
	private void processWithoutCheck(ConsumerRecord<String, Client> record, String idClient, StringBuilder operation_uuid) {
		for (int count = 0; count < record.value().getPayments().size(); count++) {
			System.out.println("*****************************************************************");
			System.out.println("Processando produto: " + record.value().getPayments().get(count).getNameProduct());

			operation_uuid.append(record.value().getPayments().get(count).getUuid_operation());				

				// Limpa as variaveis para processamento
				cleanValues();

				// Adiciona operationa_uuid para consulta
				operationQuery.append(record.value().getPayments().get(count).getUuid_operation());
				clientQuery.append(record.key());
				// Chama a api de command do produto
				commandPaymentProduct(record.key(), record.value().getPayments().get(count));

				System.out.println("Aguardando retorno conta corrente");
				// Escuta o evento de déibto automatico
				// getEventProduct(idClient, operation_uuid.toString());
				getEventProduct(record.key());
		}
	}

	private static boolean isRecordAlreadyProcessed(String idClient, String idOperation) {
		ReadOnlyKeyValueStore<String, DebitProcessed> keyValueStore = streamsClient
				.store(StoreQueryParameters.fromNameAndType("debitProcessed", QueryableStoreTypes.keyValueStore()));

		System.out.println("Checando duplicidade para o operation_uuid: " + idOperation);
		
		while (streamsClient.state() != KafkaStreams.State.RUNNING) {
			System.out.println("Waiting for kafka execution state!");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		DebitProcessed debitProcessed = keyValueStore.get(idClient);

		if (debitProcessed != null) {
			if (debitProcessed.isProcessed(idOperation)) {
				System.out.println("Registro já processado!");
				return true;
			}
		}

		return false;
	}

	private static void commandPaymentProduct(String idclient, Payment payment) {

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		Operation operation = new Operation(idclient, payment.getUuid_operation(), payment.getNameProduct());
		List<String> ports = new ArrayList<>();
		
		ports.add("8080");
		ports.add("8081");
		ports.add("8082");		
		
		String host = "http://localhost:" + ports.get(indice) + payment.getRouter();

		indice++;
		
		if(indice == 3) {
			indice = 0;
		}		
		
		URI uri = null;
		try {
			uri = new URI(host);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		HttpEntity<Operation> httpEntity = new HttpEntity<>(operation, headers);

		RestTemplate restTemplate = new RestTemplate();
		restTemplate.postForObject(uri, httpEntity, ProcessReturn.class);

		System.out.println("Chamando produto: " + payment.getRouter());
	}
	
	private static void getEventProduct(String idClient, String idOperation) {
		int x = 0;
		for (var count = 0; count < 60; count++) {
			if (queryOperations.size() > 0) {
				count = 60;
				System.out.println("Debito capturado: " + idOperation);
			} else {
				try {
					Thread.sleep(1000);
					x++;
				} catch (InterruptedException e) {
					System.out.println("erro: " + e.getMessage());
				}
			}
		}

		if (x == 60) {
			System.out.println("Tempo excedido para o processamento!");
		}
	}

	private static void getEventProduct(String idClient) {
		int x = 0;
		for (var count = 0; count < 60; count++) {
			if (queryOperations.size() > 0 && queryOperations.get(0).getIdclient().contains(idClient)) {
				count = 60;
				System.out.println("Debito capturado: " + queryOperations.get(0));
			} else {
				try {
					Thread.sleep(1000);
					x++;
				} catch (InterruptedException e) {
					System.out.println("erro: " + e.getMessage());
				}
			}
		}

		if (x == 60) {
			System.out.println("Tempo excedido para o processamento!");
		}
		
	}

	
	private static void buildTopologyStreamDebit() throws IOException {
		
		StreamsBuilder builder = new StreamsBuilder();
		paymentStream = builder.stream("ORQUESTRATION_NEW_CLIENT", Consumed.with(Serdes.String(), new ClientSerdes()));	
			
		
		if(isKafkaStream) {
			paymentStream.mapValues((key, value) -> processStreamWithCheck(value.getIdClient(), value.getPayments()));			
		}			
		
		debitEvents = builder
				.stream("ORQUESTRATION_NEW_DEBIT", Consumed.with(Serdes.String(), new DebitSerdes()))
				.selectKey((k, v) -> v.getIdclient());
		
		
		KTable<String, Client> paymentsClient = null;
		paymentsClient = paymentStream.toTable();
		
		Joined<String, Debit, Client> clientJoinParams = Joined.with(Serdes.String(), new DebitSerdes(),
				new ClientSerdes());	
		
		ValueJoiner<Debit, Client, ClientWithDebit> debitClientJoiner = (debit, client) -> new ClientWithDebit(client, debit);

		KStream<String, ClientWithDebit> withClient = debitEvents.join(paymentsClient, debitClientJoiner, clientJoinParams);
		
		withClient.foreach((key, value) -> {System.out.println("value capturado: " + value); queryOperations.add(value.getDebit());});	
		
		KGroupedStream<String, Debit> grouped = debitEvents.groupByKey(); 
		
		Initializer<DebitProcessed> debitProcessedInitializer = DebitProcessed::new;
	
		Aggregator<String, Debit, DebitProcessed> debitProcessedAdder = (key, value, aggregate) -> aggregate.addOperation(value);
		
		grouped.aggregate(debitProcessedInitializer, debitProcessedAdder, Materialized.<String, DebitProcessed, KeyValueStore<Bytes, byte[]>>
		as("debitProcessed")
		.withKeySerde(Serdes.String())
		.withValueSerde(new DebitProcessedSerdes()));		
		
		Topology topology = builder.build();		
		streamsClient = new KafkaStreams(topology, getPropertiesDebit());
		System.out.println("Topology: " + topology.describe());
	}
	
	private static void cleanValues() {
		operationQuery.delete(0, operationQuery.length());
		clientQuery.delete(0, clientQuery.length());
		queryOperations.clear();
	}


	private static Properties getPropertiesDebit() {		

		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "execution_payment");			
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, DebitSerdes.class);		
		streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, DebitTimestampExtractor.class);
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		

		Path stateDirectory = null;
		try {
			stateDirectory = Files.createTempDirectory("kafka-streams");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toAbsolutePath().toString());
		return streamsConfiguration;
	}
	
}
