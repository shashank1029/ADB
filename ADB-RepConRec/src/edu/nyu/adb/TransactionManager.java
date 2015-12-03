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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Queue;
import java.util.SortedMap;
import java.util.SortedSet;

import edu.nyu.adb.Lock.lockType;

public class TransactionManager {
	private static BufferedReader br;
	private static BufferedWriter bw;
	private static TransactionManager instance;
	private static ArrayList<Transaction> transactionList;
	private static Queue<Transaction> transactionWaitingList;
	private static boolean anyTransactionsWaiting=false;
	private static HashMap<String,Transaction> transactionMap;
	private int currentTimeStamp=0;
	public static void init(InputStream in, OutputStream out) {
		 BufferedInputStream bi=new BufferedInputStream(in);
		 br=new BufferedReader(new InputStreamReader(bi));
		 BufferedOutputStream bo=new BufferedOutputStream(out);
		 bw=new BufferedWriter(new OutputStreamWriter(bo) );
		 transactionList=new ArrayList<Transaction>();
		 transactionMap=new HashMap<>();
		 transactionWaitingList=new ArrayDeque<>();
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
				operations[i]=operations[i].trim();
				doOperation(operations[i]);
			}
			if(anyTransactionsWaiting){
				Queue<Transaction> tempQueue=new ArrayDeque<>();
				while(!transactionWaitingList.isEmpty()){
					Transaction tr=transactionWaitingList.poll();
					if(tr.isWaiting && !tempQueue.contains(tr)){
						if(!doOperation(tr.operationWaiting)){
							anyTransactionsWaiting=true;
							tempQueue.add(tr);
						}else{
							tr.isWaiting=false;
						}
					}
				}
				transactionWaitingList.addAll(tempQueue);
			}
			currentTimeStamp++;
		}
		bw.flush();
	}
	
	private boolean doOperation(String operation) throws Exception {
		if(operation.startsWith("beginRO")){
			String transactionName=operation.substring(operation.indexOf("(")+1, operation.indexOf(")"));
			Transaction t=new Transaction(transactionName,currentTimeStamp);
			t.readonlyTransaction=true;
			transactionMap.put(transactionName, t);
			transactionList.add(t);
			return true;
		}else if(operation.startsWith("begin")){
			String transactionName=operation.substring(operation.indexOf("(")+1, operation.indexOf(")"));
			Transaction t=new Transaction(transactionName,currentTimeStamp);
			transactionMap.put(transactionName, t);
			transactionList.add(t);
			return true;
		}else if(operation.startsWith("R(")){
			String transactionName=operation.substring(operation.indexOf("(")+1, operation.indexOf(","));
			Transaction t=transactionMap.get(transactionName);
			if(t.isRunning){
				String dataItem=operation.substring(operation.indexOf(",")+1, operation.indexOf(")"));
				t.operationsList.add(operation);
				Integer val=readDataItem(t,dataItem);
				if(val==null){
					t.operationWaiting=operation;
					anyTransactionsWaiting=true;
					return false;
				}else
					bw.write("Transaction "+t.transactionName+" Value of "+dataItem +": "+val+"\n");
				
			}
			bw.flush();
			return true;
		}else if(operation.startsWith("W(")){
			String transactionName=operation.substring(operation.indexOf("(")+1, operation.indexOf(","));
			Transaction t=transactionMap.get(transactionName);
			if(t.isRunning){
				String dataItem=operation.substring(operation.indexOf(",")+1, operation.lastIndexOf(","));
				int value=Integer.parseInt(operation.substring(operation.lastIndexOf(",")+1, operation.indexOf(")")));
				writeToAllSites(t,dataItem,value,operation);
				if(t.isWaiting)
					return false;
			}
			return true;
		}else if(operation.startsWith("dump")){
			if(operation.equalsIgnoreCase("dump()")){
				dump();
			}else if(operation.startsWith("dump(x")){
				String dataItem=operation.substring(operation.indexOf("(")+1, operation.indexOf(")"));
				dump(dataItem);
			}else {
				int siteId=Integer.parseInt(operation.substring(operation.indexOf("(")+1, operation.indexOf(")")));
				dump(siteId);
			}
			bw.flush();
			return true;
		}else if(operation.startsWith("end(")){
			String transactionName=operation.substring(operation.indexOf("(")+1, operation.indexOf(")"));
			Transaction t=transactionMap.get(transactionName);
			endTransaction(t);
			return true;
		}else if(operation.startsWith("fail(")){
			String siteName=operation.substring(operation.indexOf("(")+1, operation.indexOf(")"));
			SiteManager.siteList.get(Integer.parseInt(siteName)-1).failSite(currentTimeStamp);
			return true;
		}else if(operation.startsWith("recover(")){
			String siteName=operation.substring(operation.indexOf("(")+1, operation.indexOf(")"));
			SiteManager.siteList.get(Integer.parseInt(siteName)-1).recoverSite(currentTimeStamp);
			return true;
		}else if(operation.startsWith("\\")||operation.isEmpty()){
			//Do nothing
		}
		return false;
	}
	private Integer readDataItem(Transaction t, String dataItem) {
		Site availableSite=getAvailableSiteToReadFrom(dataItem); //Get sites where the dataitem is present
		if(availableSite == null){ 
			t.isWaiting=true; //There are no sites up having that dataitem then transaction needs to wait until the site is up
			transactionWaitingList.add(t);
			return null;
		}
		// Read only data items do not need locks
		if(t.readonlyTransaction){
			return availableSite.readOnlyDataItem(dataItem, t.transactionStartTimestamp);
		}else{
			if(t.getLock(availableSite,dataItem, lockType.READ_LOCK)){
				return availableSite.readDataItem(dataItem);
			}else{
				t.isWaiting=true;
				transactionWaitingList.add(t);
				for(Transaction t1:transactionList){
					if(!t1.transactionName.equalsIgnoreCase(t.transactionName) && t1.lockOnDataItems.contains(dataItem)){
						if(t1.transactionStartTimestamp < t.transactionStartTimestamp){
							t.abort("Transaction "+t1.transactionName+" is older than "+t.transactionName);
						}else{
							//t1.abort("Transaction "+t.transactionName+" is older than "+t1.transactionName);
							t.isWaiting=true;
							transactionWaitingList.add(t);
							anyTransactionsWaiting=true;
							//writeToAllSites(t,dataItem,value);
						}
						//break;
					}
				}
				return null;
			}
		}
	}
	
	private Site getAvailableSiteToReadFrom(String dataItem) {
		ArrayList<Site> siteList=getSitesContainingAvailableDataitem(dataItem);
		for(Site site:siteList){
			if(site.isUp){
				return site;
			}
		}
		return null;
	}
	private ArrayList<Site> getSitesContainingAvailableDataitem(String dataItem) {
		ArrayList<Site> sites=new ArrayList<>();
		for(Site site:SiteManager.siteList){
			if(site.dataItems.containsKey(dataItem)){
				if(site.dataItems.get(dataItem).availablForRead)
					sites.add(site);
			}
		}
		return sites;
	}
	private void endTransaction(Transaction t) {
		if(t.isRunning){
			if(t.isWaiting){ //Abort Transaction if not all sites have been up since it accessed dataitems
				t.abort("Transaction "+t.transactionName+" has some unfinished operation");
				//System.out.println("Aborting transaction "+t.transactionName);
			}else{
				t.commit(currentTimeStamp);
				//System.out.println("Commiting transaction "+t.transactionName);
			}
		}
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
	private void writeToAllSites(Transaction t, String dataItem, int value,String operation) {
		/*if(t.lockOnDataItems.contains(dataItem)){
			for(Site s:SiteManager.siteList){
				s.writeDataItem(dataItem, value,currentTimeStamp);
			}
		}else{*/
			ArrayList<Site> sites=getSitesContainingDataitem(dataItem);
			boolean gettingLockSuccessful=t.getLock(dataItem, lockType.WRITE_LOCK);
			if(gettingLockSuccessful){
				t.isWaiting=false;
				for(Site s:sites){
					s.writeDataItem(dataItem, value,currentTimeStamp);
				}
			}else{
					for(Transaction t1:transactionList){
						if(!t1.transactionName.equalsIgnoreCase(t.transactionName) && t1.lockOnDataItems.contains(dataItem)){
							if(t1.transactionStartTimestamp < t.transactionStartTimestamp){
								t.abort("Transaction "+t1.transactionName+" is older than "+t.transactionName);
							}else{
								//t1.abort("Transaction "+t.transactionName+" is older than "+t1.transactionName);
								t.isWaiting=true;
								transactionWaitingList.add(t);
								t.operationWaiting=operation;
								anyTransactionsWaiting=true;
								//writeToAllSites(t,dataItem,value);
							}
							//break;
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
			Collections.sort(s.dataItems.get(x).valueList);
			bw.write(s.dataItems.get(x).valueList.get(0).toString());
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
				Collections.sort(site.dataItems.get(dataItem).valueList);
				bw.write(site.dataItems.get(dataItem).valueList.get(0).toString());
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
