package edu.nyu.adb;

import java.io.InputStream;
import java.io.OutputStream;

public class TransactionManager {

	private static TransactionManager instance=null;
	
	public static void init(InputStream in, OutputStream out) {
		
	}
	protected TransactionManager(){
		
	}
	//Singleton
	public static TransactionManager getInstance(){
		if(instance==null){
			instance=new TransactionManager();
		}
		return instance;
	}
}
