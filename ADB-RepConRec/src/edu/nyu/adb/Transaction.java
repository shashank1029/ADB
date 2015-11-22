package edu.nyu.adb;

import java.util.ArrayList;

public class Transaction {

	public int transactionId;
	public String transactionName;
	public int transactionStartTimestamp;
	public ArrayList<String> operationsList;
	public boolean readonlyTransaction=false;
	public ArrayList<String> lockOnDataItems;
	public ArrayList<Site> sitesAccessed;
	
	public Transaction(String name,int timestamp){
		transactionStartTimestamp=timestamp;
		transactionName=name;
		lockOnDataItems=new ArrayList<>();
		operationsList=new ArrayList<>();
		sitesAccessed=new ArrayList<>();
	}

	public boolean getLock(String dataitem) {
		ArrayList<Site> sitesContainingDataitem=TransactionManager.getInstance().getSitesContainingDataitem(dataitem);
		boolean successfullyAcquiredAllLocks=true;
		for(Site s: sitesContainingDataitem){
			if(!s.lockDataItem(dataitem)){
				successfullyAcquiredAllLocks=false;
			}else{
				sitesAccessed.add(s);
			}
		}
		if(successfullyAcquiredAllLocks){
			lockOnDataItems.add(dataitem);
		}
		return successfullyAcquiredAllLocks;
	}
	
	public void abort(){
		
	}

	public void releaseLocks() {
		for(String dataitem:lockOnDataItems){
			ArrayList<Site> sitesAccessed=TransactionManager.getInstance().getSitesContainingDataitem(dataitem);
			for(Site s: sitesAccessed){
				s.lockTable.remove(dataitem);
			}
		}
	}
}
