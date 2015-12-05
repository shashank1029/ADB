package edu.nyu.adb;

import java.util.ArrayList;

public class DataItem  {

	public String dataIdentifier;
	public boolean isReplicated;
	public boolean availablForRead;
	public ArrayList<Value> valueList;
	
	/**
	 * Data Item Constructor
	 */
	public DataItem(){
		valueList=new ArrayList<>();
	}
	
}
