package edu.nyu.adb;

import java.util.ArrayList;

public class SiteManager {

	public static ArrayList<Site> siteList;
	public static void init(){
		siteList=new ArrayList<>();
		for(int i=0;i<10;i++){
			Site s=new Site(i);
			for(int j=1;j<=20;j++){
				if(j%2==0){
					s.dataItems.put(j,100+j);
				}else{
					
				}
			}
			siteList.add(s);
		}
	}
	
}	
