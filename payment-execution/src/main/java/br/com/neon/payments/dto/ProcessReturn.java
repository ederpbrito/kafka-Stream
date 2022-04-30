package br.com.neon.payments.dto;

public class ProcessReturn {

	private String nameProduct;
	private String status;
	private String uuid_operation;
	
	public ProcessReturn(){
		
	}
	
	public ProcessReturn(String nameProduct, String status, String uuid_operation) {		
		this.nameProduct = nameProduct;
		this.status = status;
		this.uuid_operation = uuid_operation;
	}

	public String getNameProduct() {
		return nameProduct;
	}

	public String getStatus() {
		return status;
	}

	public String getUuid_operation() {
		return uuid_operation;
	}
}
