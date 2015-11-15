package edu.nyu.adb;

import java.util.HashMap;

public class Site {
	
	public int id;
	public boolean isUp;
	public final HashMap<Integer, Integer> dataItems;
	public final HashMap<Integer,Boolean> lockTable;
	
	public Site(int idNumber){
		id=idNumber;
		dataItems=new HashMap<>();
		isUp=true;
		lockTable=new HashMap<>();
	}
	
	public void failSite(){
		isUp=false;
		lockTable.clear();
	}
	public void recoverSite(){
		isUp=true;
	}
	

}
