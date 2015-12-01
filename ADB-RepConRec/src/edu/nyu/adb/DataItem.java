package edu.nyu.adb;

public class DataItem implements Comparable<DataItem> {

	public int value;
	public int timestamp;
	public String dataIdentifier;
	@Override
	public int compareTo(DataItem arg0) {
		if(arg0.timestamp < timestamp)
			return -1;
		else if (arg0.timestamp == timestamp)
			return 0;
		else return 1;
	}
	
	public String toString(){
		return value+"";
	}
}
