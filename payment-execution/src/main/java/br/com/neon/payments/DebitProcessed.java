package br.com.neon.payments;

import java.util.ArrayList;
import java.util.List;

import br.com.neon.payments.domain.Debit;

public class DebitProcessed {

	
	private List<String> operations;
	
	public DebitProcessed() {
		operations = new ArrayList<>();
	}
	
	public DebitProcessed addOperation(Debit operation) {
		operations.add(operation.getUuid_operation());
		
		return this;
	}

	public boolean isProcessed(String operation) {
		return operations.contains(operation);
	}
	
	
	
}
