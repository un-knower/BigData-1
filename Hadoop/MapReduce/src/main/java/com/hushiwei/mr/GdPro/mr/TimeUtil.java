package com.hushiwei.mr.GdPro.mr;

import java.util.ArrayList;
import java.util.List;

public class TimeUtil {
	
	public static List<String> getTimeSplit(String e,String s){
		int ess = 0;
		ess = TimeUtil.getSeconds(e);
		int sss = 0;
		sss = TimeUtil.getSeconds(s);

		List<String> list = new ArrayList<String>();
		for(int i=sss; i<=ess; i+=60){
			if(i==0){
				int k = e.lastIndexOf(":");
				list.add(e.substring(0,k));
			}else{
				
			}
		}
		
		return list;
	}
	
	/**
	 * 根据年:月:日，获得秒数
	 * @param time
	 * @return
	 */
	public static Integer getSeconds(String time){
		int ess = 0;
		if(time!=null){
			String[] es = time.split(":");
			if(es!=null && es.length>0){
				ess = Integer.parseInt(es[0])*3600+Integer.parseInt(es[1])*60+Integer.parseInt(es[2]);
			}
		}
		return ess;
	}
}
