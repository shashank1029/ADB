package edu.nyu.adb;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;


public class MainProgram {

	public static void main(String[] args) throws Exception{
		if(args.length > 0){
			if(args.length == 1){
				init(new FileInputStream(args[0]),System.out);
			}else{
				init(new FileInputStream(args[0]),new FileOutputStream(args[1]));
			}
		}else{
			init(System.in,System.out);
		}
	}
	
	public static void init(InputStream in, OutputStream out) throws Exception{
		try{
			TransactionManager.init(in,out);
		}catch(Exception e){
			throw new Exception("Initialization of the Transaction manager failed");
		}
		try{
			SiteManager.init();
		}catch(Exception e){
			throw new Exception("Initialization of the Site Manager failed");
		}
	}
}
