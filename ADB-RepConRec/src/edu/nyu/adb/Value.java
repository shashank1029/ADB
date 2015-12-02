package edu.nyu.adb;

public class Value implements Comparable<Value>{

	public int value;
	public int timestamp;
	
	@Override
	public int compareTo(Value arg0) {
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
