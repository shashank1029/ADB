package edu.nyu.adb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class Site {
	
	public int id;
	public boolean isUp;
	//public final HashMap<String, DataItem> dataItems;
	public final HashMap<String, ArrayList<DataItem>> dataItems;
	public final HashMap<String, ArrayList<DataItem>> dataItemsTempStorage;
	public final HashMap<String,Boolean> lockTable;
	public int timestampAtWhichSiteFailed;
	public int timestampSinceItWasUp;
	
	public Site(int idNumber){
		id=idNumber;
		dataItems=new HashMap<>();
		isUp=true;
		lockTable=new HashMap<>();
		timestampSinceItWasUp=0;
		dataItemsTempStorage=new HashMap<>();
	}
	
	public void failSite(int timestamp){
		isUp=false;
		timestampAtWhichSiteFailed=timestamp;
		lockTable.clear();
	}
	public void recoverSite(int timestamp){
		isUp=true;
		timestampSinceItWasUp=timestamp;
	}
	
	public boolean lockDataItem(String dataItem){
		if(isUp){
			if(lockTable.containsKey(dataItem)){
				if(!lockTable.get(dataItem)){
					lockTable.put(dataItem, true);
					return true;
				}else
					return false;
			}else{
				lockTable.put(dataItem, true);
				return true;
			}
		}else{
			return false;
		}
	}
	
	public int readOnlyDataItem(String dataItem, int timestamp){
		ArrayList<DataItem> diList=dataItems.get(dataItem);
		 Collections.sort(dataItems.get(diList));
		 int val=0;
		for(DataItem di:diList){
			if(di.timestamp < timestamp){
				val=di.value;
			}
		}
		return val;
	}
	
	public void writeDataItem(String dataItem, int newData, int timestamp){
		DataItem di=new DataItem();
		di.dataIdentifier=dataItem;
		di.timestamp=timestamp;
		di.value=newData;
		dataItemsTempStorage.get(dataItem).add(di);
	}

}
