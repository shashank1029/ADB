package edu.nyu.adb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.SortedSet;

public class TransactionManager {
	private static BufferedReader br;
	private static BufferedWriter bw;
	private static TransactionManager instance;
	private static ArrayList<Transaction> transactionList;
	private static HashMap<String,Transaction> transactionMap;
	private int currentTimeStamp=0;
	public static void init(InputStream in, OutputStream out) {
		 BufferedInputStream bi=new BufferedInputStream(in);
		 br=new BufferedReader(new InputStreamReader(bi));
		 BufferedOutputStream bo=new BufferedOutputStream(out);
		 bw=new BufferedWriter(new OutputStreamWriter(bo) );
		 transactionList=new ArrayList<Transaction>();
		 transactionMap=new HashMap<>();
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
	
	public void run() throws Exception{
		String line=null;
		while((line=br.readLine())!=null){
			String[] operations=line.split(";");
			for(int i=0;i< operations.length;i++){
				if(operations[i].startsWith("begin")){
					String transactionName=operations[i].substring(operations[i].indexOf("(")+1, operations[i].indexOf(")"));
					Transaction t=new Transaction(transactionName,currentTimeStamp);
					transactionMap.put(transactionName, t);
					transactionList.add(t);
				}else if(operations[i].startsWith("beginRO")){
					String transactionName=operations[i].substring(operations[i].indexOf("(")+1, operations[i].indexOf(")"));
					Transaction t=new Transaction(transactionName,currentTimeStamp);
					t.readonlyTransaction=true;
					transactionMap.put(transactionName, t);
					transactionList.add(t);
				}else if(operations[i].startsWith("R(")){
					String transactionName=operations[i].substring(operations[i].indexOf("(")+1, operations[i].indexOf(","));
					Transaction t=transactionMap.get(transactionName);
					String dataItem=operations[i].substring(operations[i].indexOf(",")+1, operations[i].indexOf(")"));
					t.operationsList.add(operations[i]);
					readDataItem(t,dataItem);
				}else if(operations[i].startsWith("W(")){
					String transactionName=operations[i].substring(operations[i].indexOf("(")+1, operations[i].indexOf(","));
					Transaction t=transactionMap.get(transactionName);
					String dataItem=operations[i].substring(operations[i].indexOf(",")+1, operations[i].lastIndexOf(","));
					int value=Integer.parseInt(operations[i].substring(operations[i].lastIndexOf(",")+1, operations[i].indexOf(")")));
					writeToAllSites(t,dataItem,value);
				}else if(operations[i].startsWith("dump")){
					if(operations[i].equalsIgnoreCase("dump()")){
						dump();
					}else if(operations[i].startsWith("dump(x")){
						String dataItem=operations[i].substring(operations[i].indexOf("(")+1, operations[i].indexOf(")"));
						dump(dataItem);
					}else {
						int siteId=Integer.parseInt(operations[i].substring(operations[i].indexOf("(")+1, operations[i].indexOf(")")));
						dump(siteId);
					}
				}else if(operations[i].startsWith("end(")){
					String transactionName=operations[i].substring(operations[i].indexOf("(")+1, operations[i].indexOf(")"));
					Transaction t=transactionMap.get(transactionName);
					endTransaction(t);
				}else if(operations[i].startsWith("fail(")){
					String siteName=operations[i].substring(operations[i].indexOf("(")+1, operations[i].indexOf(")"));
					SiteManager.siteList.get(Integer.parseInt(siteName)-1).failSite(currentTimeStamp);
				}else if(operations[i].startsWith("recover(")){
					String siteName=operations[i].substring(operations[i].indexOf("(")+1, operations[i].indexOf(")"));
					SiteManager.siteList.get(Integer.parseInt(siteName)-1).recoverSite(currentTimeStamp);
				}
			}
			currentTimeStamp++;
		}
		bw.flush();
	}
	
	private void readDataItem(Transaction t, String dataItem) {
		// TODO Auto-generated method stub
		if(t.readonlyTransaction){
			
		}else{
			
		}
		
	}
	private void endTransaction(Transaction t) {
		//1. check if all the sites have been up since it accessed the dataitems
		boolean allSitesUp=true;
		for(Site s: t.sitesAccessed){
			if(s.isUp && s.timestampSinceItWasUp <= t.transactionStartTimestamp){
				
			}else{
				allSitesUp=false;
			}
		}
		if(!allSitesUp){ //Abort Transaction if not all sites have been up since it accessed dataitems
			t.abort();
			System.out.println("Aborting transaction "+t.transactionName);
		}else{
			//t.commit();
		}
		
		//3. Commit values
		t.releaseLocks();
		
	}
	
	public ArrayList<Site> getSitesContainingDataitem(String dataitem) {
		ArrayList<Site> sites=new ArrayList<>();
		for(Site site:SiteManager.siteList){
			if(site.dataItems.containsKey(dataitem)){
				sites.add(site);
			}
		}
		return sites;
	}
	private void writeToAllSites(Transaction t, String dataItem, int value) {
		if(t.lockOnDataItems.contains(dataItem)){
			for(Site s:SiteManager.siteList){
				s.writeDataItem(dataItem, value,currentTimeStamp);
			}
		}else{
			for(Site s:SiteManager.siteList){
				boolean gettingLockSuccessful=t.getLock(dataItem);
				if(gettingLockSuccessful){
					s.writeDataItem(dataItem, value,currentTimeStamp);
				}else{
					for(Transaction t1:transactionList){
						if(t1.transactionId!= t.transactionId && t1.lockOnDataItems.contains(dataItem)){
							if(t1.transactionStartTimestamp < t.transactionStartTimestamp){
								t.abort();
							}else{
								t1.abort();
							}
							break;
						}
					}
				}
			}
		}
		
	}
	private void dump(int siteId) throws IOException {
		Site s=SiteManager.siteList.get(siteId-1);
		bw.write("Site : "+s.id);
		bw.write("\n");
		for(String x:s.dataItems.keySet()){
			bw.write(x);
			bw.write(" - ");
			bw.write(s.dataItems.get(x).toString());
			bw.write("\n");
		}
		bw.flush();
	}
	private void dump(String dataItem) throws Exception{
		for(Site site:SiteManager.siteList){
			if(site.dataItems.containsKey(dataItem)){
				bw.write("Site :"+site.id);
				bw.write("\n");
				bw.write(dataItem);
				bw.write(" - ");
				bw.write(site.dataItems.get(dataItem).toString());
				bw.write("\n");
			}
		}
	}
	private void dump() throws IOException {
		for(Site site:SiteManager.siteList){
			dump(site.id);
			bw.write("\n");
		}
	}
}
