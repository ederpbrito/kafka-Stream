package br.com.neon.payments.domain;

import java.io.Serializable;
import java.util.List;

public class Client implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final String idclient;			
	private List<Payment> payments;	
	
	public Client(String idclient, List<Payment> payments) {		
		this.idclient = idclient;					
		this.payments = payments;
	}

	public String getIdClient() {
		return idclient;
	}

	public List<Payment> getPayments() {
		return payments;
	}
	
	
	public void addListPrioritize(List<Payment> payments) {
		this.payments = payments;
	}

	@Override
	public String toString() {
		return "Client [client_id=" + idclient + ", payments=" + payments + "]";
	}	
}
