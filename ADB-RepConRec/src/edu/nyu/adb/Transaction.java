package edu.nyu.adb;

import java.util.ArrayList;

import edu.nyu.adb.Lock.lockType;

public class Transaction {

	//public int transactionId;
	public String transactionName;
	public int transactionStartTimestamp;
	public boolean isWaiting;
	public boolean isRunning;
	public ArrayList<String> operationsList;
	public boolean readonlyTransaction=false;
	public ArrayList<String> lockOnDataItems;
	public ArrayList<Site> sitesAccessed;
	public String operationWaiting;
 	
	public Transaction(String name,int timestamp){
		transactionStartTimestamp=timestamp;
		transactionName=name;
		lockOnDataItems=new ArrayList<>();
		operationsList=new ArrayList<>();
		sitesAccessed=new ArrayList<>();
		isWaiting=false;
		isRunning=true;
	}

	public boolean getLock(String dataitem, lockType l) {
		ArrayList<Site> sitesContainingDataitem=TransactionManager.getInstance().getSitesContainingDataitem(dataitem);
		boolean successfullyAcquiredAllLocks=true;
		for(Site s: sitesContainingDataitem){
			if(s.isUp){
				if(!s.lockDataItem(dataitem, l,this)){
					successfullyAcquiredAllLocks=false;
				}else{
					sitesAccessed.add(s);
				}
			}
		}
		if(successfullyAcquiredAllLocks){
			lockOnDataItems.add(dataitem);
		}
		return successfullyAcquiredAllLocks;
	}
	
	public void abort(String whyMessage){
		for(String dataItem:lockOnDataItems){
			ArrayList<Site> sitesAccessed=TransactionManager.getInstance().getSitesContainingDataitem(dataItem);
			for(Site s: sitesAccessed){
				s.dataItemsBufferStorage.remove(dataItem);
			}
		}
		releaseLocks();
		isRunning=false;
		System.out.println("Aborting transaction "+transactionName+" because "+whyMessage);
	}

	public void releaseLocks() {
		for(String dataitem:lockOnDataItems){
			for(Site s: sitesAccessed){
				s.lockTable.remove(dataitem);
			}
		}
	}

	public void commit() {
		for(String dataItem:lockOnDataItems){
			for(Site s: sitesAccessed){
				ArrayList<DataItem> dataItemList=s.dataItems.get(dataItem);
				if(!s.dataItemsBufferStorage.isEmpty()&& dataItemList!=null){
					dataItemList.add(s.dataItemsBufferStorage.get(dataItem));
					s.dataItemsBufferStorage.remove(dataItem);
				}
			}
		}
		releaseLocks();
		isRunning=false;
		System.out.println("Commiting transaction "+transactionName);
	}
}
