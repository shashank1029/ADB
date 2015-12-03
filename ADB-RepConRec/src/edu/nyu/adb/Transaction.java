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
				if(!getLock(s, dataitem,l)){
					successfullyAcquiredAllLocks=false;
				}
		}
		if(successfullyAcquiredAllLocks){
			if(!lockOnDataItems.contains(dataitem))
				lockOnDataItems.add(dataitem);
		}
		return successfullyAcquiredAllLocks;
	}
	
	public boolean getLock(Site siteToReadFrom,String dataitem, lockType l) {
		boolean successfullyAcquiredAllLocks=true;
			if(siteToReadFrom.isUp){
				if(!siteToReadFrom.lockDataItem(dataitem, l,this)){
					successfullyAcquiredAllLocks=false;
				}else{
					if(!sitesAccessed.contains(siteToReadFrom))
						sitesAccessed.add(siteToReadFrom);
				}
			}
		if(successfullyAcquiredAllLocks){
			if(!lockOnDataItems.contains(dataitem))
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

	public void commit(int currentTtimestamp) {
		for(String dataItem:lockOnDataItems){
			for(Site s: sitesAccessed){
				if(!s.dataItemsBufferStorage.isEmpty()&& s.dataItems.containsKey(dataItem) && s.dataItemsBufferStorage.containsKey(dataItem)){
					DataItem dItem=s.dataItems.get(dataItem);
					ArrayList<Value> dataItemValueList=dItem.valueList;
					dItem.availablForRead=s.dataItemsBufferStorage.get(dataItem).availablForRead;
					for(Value v:s.dataItemsBufferStorage.get(dataItem).valueList){
						v.timestamp=currentTtimestamp;
					}
					dataItemValueList.addAll(s.dataItemsBufferStorage.get(dataItem).valueList);
					s.dataItemsBufferStorage.remove(dataItem);
				}
			}
		}
		releaseLocks();
		isRunning=false;
		System.out.println("Commiting transaction "+transactionName);
	}
}
