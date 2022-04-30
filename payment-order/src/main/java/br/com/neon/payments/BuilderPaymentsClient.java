package br.com.neon.payments;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.github.javafaker.Faker;

public class BuilderPaymentsClient {

	
	public static Client create() {

		Faker faker = new Faker();
		Client client = new Client(faker.idNumber().valid().toString());
		client.addListPrioritize(generateList(client.getIdclient()));

		return client;
	}

	@SuppressWarnings("unlikely-arg-type")
	private static List<Payment> generateList(String idclient) {

		List<Payment> list = new ArrayList<>();
		int r = (int) (Math.random() * 5);

		int count = r;

		for (var x = 0; x < count; x++) {
			int r1 = (int) (Math.random() * 4);
			String name = new String[] { "COBRANCA", "EP", "PIX", "BOLETO" }[r1];

			if(!list.contains(name)) {				
				list.add(new Payment(idclient,UUID.randomUUID().toString(), getRouteProduct(name), name));
			}else {
				x--;
			}
		}
		
		if(list.size() == 0) {			
			list.add(new Payment(idclient,UUID.randomUUID().toString(), getRouteProduct("COBRANCA"), "COBRANCA"));
		}

		return list;
	}
	
	private static String getRouteProduct(String product) {
		String prod;
		switch(product) {
		case"COBRANCA": prod="/payment/billing";break;
		case"PIX": prod="/payment/pix";break;
		case"EP": prod="/payment/emprestimopessaol";break;
		case"BOLETO": prod="/payment/boleto";break;
		default:
			throw new RuntimeException( "Product not found");
		}
		
		return prod;
	}


}
