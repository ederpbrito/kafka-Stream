package br.com.neon.payments.domain;

public class Payment {
	
	private String idclient;
	private String uuid_operation;	
	private String router;
	private String nameproduct;
	private String timestamp;
	
	

	public Payment(String idclient, String uuid_operation, String router, String nameproduct, String timestamp) {		
		this.idclient = idclient;
		this.uuid_operation = uuid_operation;
		this.router = router;
		this.nameproduct = nameproduct;
		this.timestamp = timestamp;
	}	

	public String getIdclient() {
		return idclient;
	}

	public String getUuid_operation() {
		return uuid_operation;
	}

	public String getRouter() {
		return router;
	}

	public String getNameProduct() {
		return nameproduct;
	}	

	public String getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "Payment [idclient=" + idclient + ", uuid_operation=" + uuid_operation + ", router=" + router
				+ ", nameProduct=" + nameproduct + ", timestamp=" + timestamp + "]";
	}

	
}
