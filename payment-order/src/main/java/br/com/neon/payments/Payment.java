package br.com.neon.payments;

import java.io.Serializable;

public class Payment implements Serializable {
		
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final String idclient;
	private final String uuid_operation;	
	private final String router;
	private final String nameproduct;
	
	public Payment(String idclient, String uuid_operation, String router, String nameProduct) {	
		this.idclient = idclient;
		this.uuid_operation = uuid_operation;		
		this.router = router;
		this.nameproduct = nameProduct;
	}

	public String getUuid_operation() {
		return uuid_operation;
	}

	public String getRouter() {
		return router;
	}

	public String getNameproduct() {
		return nameproduct;
	}

	public String getIdclient() {
		return idclient;
	}
	
	
	
}
