package edu.nyu.adb;

import java.util.ArrayList;

public class SiteManager {

	public static ArrayList<Site> siteList;
	
	/**
	 * Initialize all sites
	 */
	public static void init(){
		siteList=new ArrayList<>();
		for(int i=1;i<=10;i++){
			Site s=new Site(i);
			for(int j=1;j<=20;j++){
				//Add even data items to all sites
				if(j%2==0){
					DataItem dt=new DataItem();
					Value v=new Value();
					v.timestamp=0;
					v.value=100+j;
					dt.isReplicated=true;
					dt.availablForRead=true;
					dt.dataIdentifier="x"+j;
					dt.valueList.add(v);
					s.dataItems.put(dt.dataIdentifier,dt);
				}else{
					//Add odd data items to one site each 
					if(i==1+(j%10)){
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
			siteList.add(s);
		}
	}
	
}	
