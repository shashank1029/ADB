package edu.nyu.adb;

import java.util.ArrayList;

public class SiteManager {

	public static ArrayList<Site> siteList;
	
	/**
	 * Initialize all sites
	 */
	public static void init(){
		siteList=new ArrayList<>();
		for(int i=1;i<=10;i++){ //Creating 10 new sites
			Site s=new Site(i); 
			for(int j=1;j<=20;j++){
				//Add even data items to all sites
				if(j%2==0){
					//Even data items are replicated
					DataItem dt=new DataItem();
					Value v=new Value();
					v.timestamp=0; //Set the initial timestamp
					v.value=100+j; //Initial value
					dt.isReplicated=true; //It is replicated
					dt.availablForRead=true; //Initially all the data items are available for read
					dt.dataIdentifier="x"+j; //Set the dataidentifier for the data item
					dt.valueList.add(v); //Add initial value to the list
					s.dataItems.put(dt.dataIdentifier,dt); //Adding the initial values to the secondary storage
				}else{
					//Add odd data items to one site each 
<<<<<<< HEAD
					if(i==1+(j%10)){ //Odd variables
=======
					if(i==1+(j%10)){
>>>>>>> refs/remotes/choose_remote_name/master
						DataItem dt=new DataItem();
						Value v=new Value();
						v.timestamp=0; 
						v.value=100+j;
						dt.dataIdentifier="x"+j;
						dt.isReplicated=false;
						dt.availablForRead=true;
						dt.valueList.add(v);
						s.dataItems.put(dt.dataIdentifier,dt);
					}
				}
			}
			siteList.add(s); //Add site to the site list so that it is maintained by the site manager
		}
	}
	
}	
