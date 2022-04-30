package br.com.neon.payments.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Debit implements Serializable {		
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	private String idclient;
	private String uuid_operation;
	private String status;
	private String description;
	private List<String> listProcessed;

	public Debit() {	
		listProcessed = new ArrayList<>();
	}

	public Debit(String idclient, String uuid_operation, String status, String description) {
		
		this.idclient = idclient;
		this.uuid_operation = uuid_operation;
		this.status = status;
		this.description = description;
	}	

	public String getIdclient() {
		return idclient;
	}

	public String getUuid_operation() {
		return uuid_operation;
	}

	public String getStatus() {
		return status;
	}

	public String getDescription() {
		return description;
	}	
	
	public void listAddProcessed(String operation) {
		listProcessed.add(operation);
	}	

	public List<String> getListProcessed() {
		return listProcessed;
	}
	
	public boolean isOperationCaptured(String Operation_uuid) {
		return listProcessed.contains(Operation_uuid);
	}
	
	public String getOperationProcessed(String Operation_uuid) {
		return listProcessed.contains(Operation_uuid) ? Operation_uuid : "";
	}
	

	@Override
	public String toString() {
		return "Debit [client=" + idclient + ", uuid_operation=" + uuid_operation + ", status=" + status
				+ ", description=" + description + ", listProcessed=" + listProcessed + "]";
	}	
}
