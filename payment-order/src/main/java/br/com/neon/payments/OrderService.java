package br.com.neon.payments;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class OrderService {

	private Map<Integer, String> mapOrderExecution;

	public OrderService() {
		mapOrderExecution= new HashMap<>();
		
		mapOrderExecution.put(1, "COBRANCA");
		mapOrderExecution.put(2, "PIX");
		mapOrderExecution.put(3, "EP");
		mapOrderExecution.put(4, "BOLETO");
	}
	
	public void dispatcher(Client client) throws InterruptedException, ExecutionException {
		
		try (var orderDispatcher = new KafkaDispatcher<Client>()) {		
			List<Payment> listPrioritize  = prioritizeToList(client.getPayments());			
			client.addListPrioritize(listPrioritize);
			orderDispatcher.send("ORQUESTRATION_NEW_CLIENT", client.getIdclient(), client);
		}
	}

	/**
	 * Method for prioritize list
	 * 1:COBRANCA 
	 * 2:PIX 
	 * 3:EP 
	 * 4:BOLETO 
	 **/
	public List<Payment> prioritizeToList(List<Payment> list) {

		List<Payment> newList = new ArrayList<>();
		
		for (Map.Entry<Integer, String> entry : mapOrderExecution.entrySet()) {
			
			List<Payment> listAux = list.stream()
					.filter(p -> p.getNameproduct().equals(entry.getValue()))
					.collect(Collectors.toList());			
			
			if(listAux.size() > 0) {
				newList.add(listAux.get(0));
			}
			
		}

		return newList;
	}
	
}
