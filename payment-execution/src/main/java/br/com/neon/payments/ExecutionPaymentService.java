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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import br.com.neon.payments.domain.Debit;
import br.com.neon.payments.domain.Operation;
import br.com.neon.payments.domain.Payment;
import br.com.neon.payments.dto.ProcessReturn;
import br.com.neon.payments.serialization.DebitSerdes;
import br.com.neon.payments.serialization.DebitTimestampExtractor;

public class ExecutionPaymentService {

	private static List<Debit> list = new ArrayList<>();
	private static KafkaStreams streams;
	private static StringBuffer operation = new StringBuffer();

	public static void main(String[] args) throws IOException {
		var ExecutionPaymentService = new ExecutionPaymentService();	
		
		//Monta o filtro de busca stream
		buildTopologyStreamDebit();
		
		streams.cleanUp();
		
		streams.start();		
		
		Map<String, String> propCustom = new HashMap<>();
		propCustom.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		
		try (var service = new KafkaService<Payment>(ExecutionPaymentService.class.getSimpleName(), "ORQUESTRATION_NEW_PAYMENT",
				record -> {					
					ExecutionPaymentService.parse(record);					
				}, Payment.class, propCustom)) {
			service.run();			
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
	
	private void parse(ConsumerRecord<String, Payment> record) {
		System.out.println("####################################################################");
		System.out.println("Orquestration payment");
		System.out.println("Key: " + record.key());
		System.out.println("Value: " + record.value());
		System.out.println("Partition: " + record.partition());
		System.out.println("Offset: " + record.offset());		
		
			System.out.println("*****************************************************************");
			System.out.println("Processando produto: " + record.value());
			
			//Limpa as variaveis para processamento
			cleanValues();

			//Adiciona operationa_uuid para consulta			
			operation.append(record.value().getUuid_operation());
			
			//Chama a api de command do produto
			commandPaymentProduct(record.key(), record.value());		
			
			System.out.println("Aguardando retorno conta corrente");
			//Escuta o evento de déibto automatico
			getEventProduct();	
			
	}	
	
	private static void commandPaymentProduct(String idclient, Payment payment) {

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		Operation operation = new Operation(idclient, payment.getUuid_operation(), payment.getNameProduct());

		URI uri = null;
		try {
			uri = new URI(payment.getRouter());
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		HttpEntity<Operation> httpEntity = new HttpEntity<>(operation, headers);

		RestTemplate restTemplate = new RestTemplate();
		restTemplate.postForObject(uri, httpEntity, ProcessReturn.class);
		
		System.out.println("Chamando produto: " + payment.getRouter());
	}

	private static void getEventProduct() {
		for (var count = 0; count < 60; count++) {
			if (list.size() > 0) {
				count = 60;				
				System.out.println("Debito capturado: " + list.get(0).getUuid_operation());				
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {					
					System.out.println("erro: " + e.getMessage());
				}
			}
		}
	}
	
	private void cleanValues() {
		operation.delete(0, operation.length());		
		list.clear();
	}
	
	private static void buildTopologyStreamDebit() throws IOException {
		StreamsBuilder builder = new StreamsBuilder();	
		
		Consumed<String, Debit> debitConsumed =
				Consumed.with(Serdes.String(), new DebitSerdes())
				.withTimestampExtractor(new DebitTimestampExtractor());		
		
		KStream<String, Debit> textLines = builder.stream("ORQUESTRATION_NEW_DEBIT", debitConsumed);

		KStream<String, Debit> filter = textLines.filter(new Predicate<String, Debit>() {
			@Override
			public boolean test(String key, Debit value) {
				return  value.getUuid_operation().contains(operation);
			}
		});	
		
		filter.foreach((key, value) -> list.add(value));

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
		
		Path stateDirectory= null;
		try {
			stateDirectory = Files.createTempDirectory("kafka-streams");
		} catch (IOException e) {			
			System.out.println("erro: " + e.getMessage());
		}
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toAbsolutePath().toString());
		return streamsConfiguration;
	}
}
