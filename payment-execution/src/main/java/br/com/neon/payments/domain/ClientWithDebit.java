package br.com.neon.payments.domain;

public class ClientWithDebit {

	private Client client;
	private Debit debit;
	
	public ClientWithDebit(Client client, Debit debit) {
		
		this.client = client;
		this.debit = debit;
	}	

	public Client getClient() {
		return client;
	}

	public void setClient(Client client) {
		this.client = client;
	}

	public Debit getDebit() {
		return debit;
	}

	public void setDebit(Debit debit) {
		this.debit = debit;
	}

	@Override
	public String toString() {
		return "ClientWithDebit [client=" + client + ", debit=" + debit + "]";
	}
	
}
