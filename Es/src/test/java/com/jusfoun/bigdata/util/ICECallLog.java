package com.jusfoun.bigdata.util;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;


/**
 * 
 * @author admin
 * * 样例： 20160415113545|INFO |getSerStateInfo|SerStateList|192.168.4.213:42608|192.168.4.212:37354|45|success
 *      时间  方法名 服务名  服务器ip  客户端ip  调用成功或失败时间   成功失败
 */
public class ICECallLog {

	private Long calltime ; //调用时间 
	private String methodName ; //调用方法
	private String serviceName; //服务名称 
	private String serverIp ;  //服务器ip 
	private String clientIp ; //客户端ip 
	private Long callLastTime ;  // 调用持续时常 
	private String sucessOrFail ; // 成功还是失败 
	
	private Integer calltimeYear ; 
	private Integer calltimeMonth ; 
	private Integer calltimeDay ; 
	private Integer calltimeHour ; 
	private Integer calltimeMinute ; 
	private Integer calltimeSecond ;
	
	
	public Long getCalltime() {
		return calltime;
	}
	public void setCalltime(Long calltime) {
		setCalltimeYMD( calltime );//设置时分秒 
		this.calltime = calltime;
	}
	public String getMethodName() {
		return methodName;
	}
	public void setMethodName(String methodName) {
		if(methodName!=null && methodName.length()>0){
			this.methodName = methodName.toLowerCase();
		}else{
			this.methodName = methodName ; 
		}
		
	}
	public String getServiceName() {
		return serviceName;
	}
	public void setServiceName(String serviceName) {
		if(serviceName!=null && serviceName.length()>0){
			this.serviceName = serviceName.toLowerCase();
		}else{
			this.serviceName = serviceName;
		}
	}
	public String getServerIp() {
		return serverIp;
	}
	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}
	public String getClientIp() {
		return clientIp;
	}
	public void setClientIp(String clientIp) {
		this.clientIp = clientIp;
	}
 
	
	public Long getCallLastTime() {
		return callLastTime;
	}
	public void setCallLastTime(Long callLastTime) {
		this.callLastTime = callLastTime;
	}
	public String getSucessOrFail() {
		return sucessOrFail;
	}
	public void setSucessOrFail(String sucessOrFail) {
		this.sucessOrFail = sucessOrFail;
	}
	public Integer getCalltimeYear() {
		return calltimeYear;
	}
	public void setCalltimeYear(Integer calltimeYear) {
		this.calltimeYear = calltimeYear;
	}
	public Integer getCalltimeMonth() {
		return calltimeMonth;
	}
	public void setCalltimeMonth(Integer calltimeMonth) {
		this.calltimeMonth = calltimeMonth;
	}
	public Integer getCalltimeDay() {
		return calltimeDay;
	}
	public void setCalltimeDay(Integer calltimeDay) {
		this.calltimeDay = calltimeDay;
	}
	public Integer getCalltimeHour() {
		return calltimeHour;
	}
	public void setCalltimeHour(Integer calltimeHour) {
		this.calltimeHour = calltimeHour;
	}
	public Integer getCalltimeMinute() {
		return calltimeMinute;
	}
	public void setCalltimeMinute(Integer calltimeMinute) {
		this.calltimeMinute = calltimeMinute;
	}
	public Integer getCalltimeSecond() {
		return calltimeSecond;
	}
	public void setCalltimeSecond(Integer calltimeSecond) {
		this.calltimeSecond = calltimeSecond;
	} 
	//拆分调用时间的年月日 
	private void setCalltimeYMD(Long calltime){
		Date da = null;
		try {
			da = DateUtils.parseDatetime(calltime+"","yyyyMMddHHmmss" );
		} catch (ParseException e) {
			e.printStackTrace();
		} 
		
		Calendar calendar = Calendar.getInstance(Locale.CHINA);  
	    calendar.setTime( da );  
	    this.setCalltimeYear( calendar.get( Calendar.YEAR ) );
	    this.setCalltimeMonth( calendar.get( Calendar.MONDAY  )+1  );
	    this.setCalltimeDay(  calendar.get( Calendar.DAY_OF_MONTH  )   );;
	    this.setCalltimeHour( calendar.get( Calendar.HOUR_OF_DAY  )    );
	    this.setCalltimeMinute( calendar.get( Calendar.MINUTE  )     );
	    this.setCalltimeSecond( calendar.get( Calendar.SECOND  )    );
	}
}
