package br.com.neon.payments.domain;

public class Operation {
	
	private final String idClient;
	private final String uuid_operation;		
	private final String nameProduct;
	
	public Operation(String idClient, String uuid_operation, String nameProduct) {		
		this.idClient = idClient;
		this.uuid_operation = uuid_operation;		
		this.nameProduct = nameProduct;
	}	

	public String getIdClient() {
		return idClient;
	}

	public String getUuid_operation() {
		return uuid_operation;
	}	

	public String getNameProduct() {
		return nameProduct;
	}	
}
