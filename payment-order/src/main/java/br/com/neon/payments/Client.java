package br.com.neon.payments;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Client implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final String idclient;			
	private List<Payment> payments;
	
	
	public Client(String idClient) {		
		this.idclient = idClient;					
		this.payments = new ArrayList<>();
	}

	public String getIdclient() {
		return idclient;
	}

	public List<Payment> getPayments() {
		return this.payments;
	}	
	
	public void addListPrioritize(List<Payment> payments) {
		this.payments = payments;
	}
	
}
