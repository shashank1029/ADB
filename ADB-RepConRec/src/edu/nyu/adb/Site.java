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
	
	public Site(int idNumber){
		id=idNumber;
		dataItems=new HashMap<>();
		isUp=true;
		lockTable=new HashMap<>();
		timestampSinceItWasUp=0;
		dataItemsBufferStorage=new HashMap<>();
	}
	
	public void failSite(int timestamp){
		isUp=false;
		timestampAtWhichSiteFailed=timestamp;
		for(String di:lockTable.keySet()){
			for(lockType lt:lockTable.get(di).keySet()){
				for(Transaction t:lockTable.get(di).get(lt)){
					t.abort("Site "+this.id+" failed.");	
				}
			}
		}
		lockTable.clear();
	}
	
	public void recoverSite(int timestamp){
		isUp=true;
		timestampSinceItWasUp=timestamp;
		for(String x:dataItems.keySet()){
			if(dataItems.get(x).isReplicated){
				dataItems.get(x).availablForRead=false;
			}else{
				dataItems.get(x).availablForRead=true;
			}
		}
	}
	
	public boolean lockDataItem(String dataItem, lockType newlt, Transaction t){
		if(isUp){
			if(lockTable.containsKey(dataItem)){
				HashMap<lockType, ArrayList<Transaction>> lockTypeOnDataItem=lockTable.get(dataItem);
				for(lockType ltOnDataItem:lockTypeOnDataItem.keySet()){
					if(ltOnDataItem==lockType.READ_LOCK && newlt==lockType.WRITE_LOCK){
						for(Transaction alreadyHoldingLocksTransaction:lockTypeOnDataItem.get(ltOnDataItem)){
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
					}else if(ltOnDataItem==lockType.READ_LOCK && newlt==lockType.READ_LOCK){
						for(Transaction alreadyHoldingLocksTransaction:lockTypeOnDataItem.get(ltOnDataItem)){
							if(t.transactionName.equalsIgnoreCase(alreadyHoldingLocksTransaction.transactionName)){
								return true;
							}else{
								HashMap<lockType, ArrayList<Transaction>> newMap=lockTable.get(dataItem);
								newMap.get(newlt).add(t);
								lockTable.put(dataItem, newMap);
								return true;
							}
						}
					}else if(ltOnDataItem==lockType.WRITE_LOCK && newlt==lockType.WRITE_LOCK){
						for(Transaction alreadyHoldingLocksTransaction:lockTypeOnDataItem.get(ltOnDataItem)){
							if(t.transactionName.equalsIgnoreCase(alreadyHoldingLocksTransaction.transactionName)){
								return true;
							}else{
								return false;
							}
						}
					}else if(ltOnDataItem==lockType.WRITE_LOCK && newlt==lockType.READ_LOCK){
						for(Transaction alreadyHoldingLocksTransaction:lockTypeOnDataItem.get(ltOnDataItem)){
							if(t.transactionName.equalsIgnoreCase(alreadyHoldingLocksTransaction.transactionName)){
								return true;
							}else{
								return false;
							}
						}
					}
				}
				
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
	
	public Integer readDataItem(String dataItem){
		if(dataItemsBufferStorage.containsKey(dataItem)){
			return dataItemsBufferStorage.get(dataItem).valueList.get(0).value;
		}else {
			Collections.sort(dataItems.get(dataItem).valueList);
			return dataItems.get(dataItem).valueList.get(0).value;
		}
		
	}
	
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
