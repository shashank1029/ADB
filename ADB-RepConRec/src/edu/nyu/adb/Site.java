package edu.nyu.adb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import edu.nyu.adb.Lock.lockType;

public class Site {
	
	public int id;
	public boolean isUp;
	//public final HashMap<String, DataItem> dataItems;
	public final HashMap<String, DataItem> dataItems;
	public final HashMap<String, DataItem> dataItemsBufferStorage;
	public final HashMap<String,HashMap<lockType, ArrayList<Transaction>>> lockTable;
	public final ArrayList<DataItem> availableDataItemsToRead=null;
	public int timestampAtWhichSiteFailed;
	public int timestampSinceItWasUp;
	
	/**
	 * Site  - public constructor
	 * @param idNumber
	 */
	public Site(int idNumber){
		id=idNumber;
		dataItems=new HashMap<>();
		isUp=true;
		lockTable=new HashMap<>();
		timestampSinceItWasUp=0;
		dataItemsBufferStorage=new HashMap<>();
	}
	
	/**
	 * Simulate a site failure
	 * @param timestamp
	 */
	public void failSite(int timestamp){
		isUp=false;
		timestampAtWhichSiteFailed=timestamp;
		for(String di:lockTable.keySet()){
			for(lockType lt:lockTable.get(di).keySet()){
				for(Transaction t:lockTable.get(di).get(lt)){
					t.abort("Site "+this.id+" failed.");	//Abort transactions due to site failure
				}
			}
		}
		lockTable.clear();
	}
	
	/**
	 * Simulate recovery of a site
	 * @param timestamp
	 */
	public void recoverSite(int timestamp){
		isUp=true;
		timestampSinceItWasUp=timestamp;
		for(String x:dataItems.keySet()){
			//Set replicated data items 
			if(dataItems.get(x).isReplicated){
				dataItems.get(x).availablForRead=false;
			}else{
				dataItems.get(x).availablForRead=true;
			}
		}
	}
	
	/**
	 * Lock data item across all sites
	 * @param dataItem
	 * @param newlt
	 * @param t
	 * @return
	 */
	public boolean lockDataItem(String dataItem, lockType newlt, Transaction t){
		if(isUp){
			//if there is an existing lock on the data item
			if(lockTable.containsKey(dataItem)){
				HashMap<lockType, ArrayList<Transaction>> lockTypeOnDataItem=lockTable.get(dataItem);
				for(lockType ltOnDataItem:lockTypeOnDataItem.keySet()){
					//If there is a read lock on the data item and new lock is write lock then change the lock type
					if(ltOnDataItem==lockType.READ_LOCK && newlt==lockType.WRITE_LOCK){
						for(Transaction alreadyHoldingLocksTransaction:lockTypeOnDataItem.get(ltOnDataItem)){
							//if same transaction earlier had a read lock on the data item, change lock type
							if(t.transactionName.equalsIgnoreCase(alreadyHoldingLocksTransaction.transactionName)){
								HashMap<lockType, ArrayList<Transaction>> newMap=new HashMap<>();
								newMap.put(newlt, new ArrayList<Transaction>());
								newMap.get(newlt).add(t);
								lockTable.put(dataItem, newMap);
								return true;
							}else{
								return false;
							}
						}
					//if old and new lock type was a read lock
					}else if(ltOnDataItem==lockType.READ_LOCK && newlt==lockType.READ_LOCK){
						for(Transaction alreadyHoldingLocksTransaction:lockTypeOnDataItem.get(ltOnDataItem)){
							//for same transaction do nothing
							if(t.transactionName.equalsIgnoreCase(alreadyHoldingLocksTransaction.transactionName)){
								return true;
							}else{
								//for a different transaction, add lock to locktable
								HashMap<lockType, ArrayList<Transaction>> newMap=lockTable.get(dataItem);
								newMap.get(newlt).add(t);
								lockTable.put(dataItem, newMap);
								return true;
							}
						}
					//if old and new lock type is write lock 
					}else if(ltOnDataItem==lockType.WRITE_LOCK && newlt==lockType.WRITE_LOCK){
						for(Transaction alreadyHoldingLocksTransaction:lockTypeOnDataItem.get(ltOnDataItem)){
							//if write lock is required by same transaction which was earlier holding the lock return true
							if(t.transactionName.equalsIgnoreCase(alreadyHoldingLocksTransaction.transactionName)){
								return true;
							}else{
								//else return false, as write locks conflict
								return false;
							}
						}
					//if old lock on data item was write lock, and nwe lock type is read lock 
					}else if(ltOnDataItem==lockType.WRITE_LOCK && newlt==lockType.READ_LOCK){
						for(Transaction alreadyHoldingLocksTransaction:lockTypeOnDataItem.get(ltOnDataItem)){
							//if write lock was by same transaction, return true
							if(t.transactionName.equalsIgnoreCase(alreadyHoldingLocksTransaction.transactionName)){
								return true;
							}else{
								//if write lock was by another transaction, return false
								return false;
							}
						}
					}
				}
			//if there is no previous lock on the data item
			}else{
				HashMap<lockType, ArrayList<Transaction>> newMap=new HashMap<>();
				newMap.put(newlt, new ArrayList<Transaction>());
				newMap.get(newlt).add(t);
				lockTable.put(dataItem, newMap);
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 
	 * @param dataItem
	 * @param timestamp
	 * @return value of data item from previous version 
	 */
	public Integer readOnlyDataItem(String dataItem, int timestamp){
		ArrayList<Value> diList=dataItems.get(dataItem).valueList;
		Collections.sort(dataItems.get(dataItem).valueList);
		Integer val=null;
		for(Value di:diList){
			if(di.timestamp < timestamp){
				val=di.value;
				break;
			}
		}
		return val;
	}
	
	/**
	 * 
	 * @param dataItem
	 * @return value of data item
	 */
	public Integer readDataItem(String dataItem){
		if(dataItemsBufferStorage.containsKey(dataItem)){
			return dataItemsBufferStorage.get(dataItem).valueList.get(0).value;
		}else {
			Collections.sort(dataItems.get(dataItem).valueList);
			return dataItems.get(dataItem).valueList.get(0).value;
		}
	}
	
	/**
	 * Write data item to site
	 * @param dataItem
	 * @param newData
	 * @param timestamp
	 */
	public void writeDataItem(String dataItem, int newData, int timestamp){
		DataItem di=new DataItem();
		di.dataIdentifier=dataItem;
		di.availablForRead=true;
		Value v=new Value();
		v.value=newData;
		di.valueList.add(v);
		dataItemsBufferStorage.put(dataItem,di);
	}

}
