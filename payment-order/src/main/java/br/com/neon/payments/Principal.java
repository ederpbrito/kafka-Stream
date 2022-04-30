package br.com.neon.payments;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Principal {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		OrderService service =  new OrderService();
		
		for (int count = 0; count < 20; count++) {
			var paymentsClient = BuilderPaymentsClient.create();
//			var payment1 = new Payment("c4c75a4e-86ac-44c7-b9cc-455ae5a81b1a","http://localhost:8080/payment/emprestimopessaol","EP");
//			var payment2 = new Payment("ddf4c05b-42bb-4877-9ab0-3b219e07a373","http://localhost:8080/payment/billing","COBRANCA");
//			var payment3 = new Payment("c4c75a4e-86ac-44c7-b9cc-455ae5a81b1a","http://localhost:8080/payment/pix","PIX");
//			List<Payment> payments = new ArrayList<>();
//			payments.add(payment1);
//			payments.add(payment2);
//			payments.add(payment3);
//			var client = new Client("359-25-6566");
//			client.addListPrioritize(payments);
			service.dispatcher(paymentsClient);
		}
	}
}
