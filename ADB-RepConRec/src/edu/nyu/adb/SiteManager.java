package edu.nyu.adb;

import java.util.ArrayList;

public class SiteManager {

	public static ArrayList<Site> siteList;
	public static void init(){
		siteList=new ArrayList<>();
		for(int i=1;i<=10;i++){
			Site s=new Site(i);
			for(int j=1;j<=20;j++){
				if(j%2==0){
					DataItem dt=new DataItem();
					dt.timestamp=0;
					dt.value=100+j;
					dt.dataIdentifier="x"+j;
					ArrayList<DataItem> x=new ArrayList<>();
					x.add(dt);
					s.dataItems.put(dt.dataIdentifier,x);
				}else{
					if(i==1+(j%10)){
						DataItem dt=new DataItem();
						dt.timestamp=0;
						dt.value=100+j;
						dt.dataIdentifier="x"+j;
						ArrayList<DataItem> x=new ArrayList<>();
						x.add(dt);
						s.dataItems.put(dt.dataIdentifier,x);
					}
				}
			}
			siteList.add(s);
		}
	}
	
}	
