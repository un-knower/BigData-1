package somethingUtils.common;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateUtil {
	/**
     * 转换成日期格式字符串
     * @param seconds 精确到秒的字符串
     * @param formatStr
     * @return
	 * @throws Exception 
     */
	public static String timeStamp2DateSecon(String dateString,String format) throws Exception {
		String regEx="[^0-9]";   
        Pattern p = Pattern.compile(regEx);   
        Matcher m = p.matcher(dateString);   
        String str = m.replaceAll("").trim();
        int len = str.length();
        if(str.length()<14){
        	for(int i=0;i<len;i++){
        		str+="0";
        	}
        }
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddhhmmss");
        SimpleDateFormat sdf2 = new SimpleDateFormat(format);
        Date date = sdf1.parse(str);//提取格式中的日期
        String newStr = sdf2.format(date); //改变格式
		return newStr;
	}
	/**
	 * 转换成日期格式字符串
	 * @param seconds 精确到日期的字符串
	 * @param formatStr
	 * @return
	 * @throws Exception 
	 */
	public static String timeStamp2DateDay(String dateString,String format) throws Exception {

		String regEx="[^0-9]";   
		Pattern p = Pattern.compile(regEx);   
		Matcher m = p.matcher(dateString);   
		String str = m.replaceAll("").trim();
	      if(str.length()>8){
        	  str = str.substring(0, 8);
          }
   
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf2 = new SimpleDateFormat(format);
		Date date = sdf1.parse(str);//提取格式中的日期
		String newStr = sdf2.format(date); //改变格式
		return newStr;
	}
	
	public String choseFormat(String string,String format) throws Exception{
	String str="";
	String[] day = {"yyyyMMdd","yyyy-MM-dd","yyyy%MM%dd"};
	String[] secon = {"yyyyMMddhhmmss","yyyy-MM-dd HH:mm:ss","yyyy%MM%dd HH%mm%ss"};
	String[] money = {"###,###"};
	DecimalFormat myformat = new DecimalFormat();
	List<String> dayList = new ArrayList<String>(); 
	List<String> seconList = new ArrayList<String>(); 
	List<String> moneyList = new ArrayList<String>(); 
	Collections.addAll(dayList, day);
	Collections.addAll(seconList, secon);
	Collections.addAll(moneyList, money);
		if(dayList.contains(format)){
			str =timeStamp2DateDay(string, format);
		}
		if(seconList.contains(format)){
			str =timeStamp2DateSecon(string, format);
		}
	
		if(moneyList.contains(format)){
			 myformat.applyPattern("###,###");	
			 str = myformat.format(Integer.parseInt(string));
		
		}
		return str;
	}
	/*
	* 创建日期 2010-04-19
	* Author  sea.jiang
	*
	* 功能  取日期时间工具
	*
	*/
	/**
	* 说明:      取日期时间工具 

	/**
	* @see    取得当前日期（格式为：yyyy-MM-dd）
	* @return String
	*/
	public String GetDate()
	{
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	String sDate = sdf.format(new Date());
	return sDate;
	}

	    /**
	* @see     取得当前时间（格式为：yyy-MM-dd HH:mm:ss）
	* @return String
	*/
	public static String GetDateTime()
	{
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	String sDate = sdf.format(new Date());
	return sDate;
	}

	    /**
	* @see     按指定格式取得当前时间()
	* @return String
	*/
	public String GetTimeFormat(String strFormat)
	{
	SimpleDateFormat sdf = new SimpleDateFormat(strFormat);
	String sDate = sdf.format(new Date());
	return sDate;
	}

	    /**
	* @see     取得指定时间的给定格式()
	* @return String
	* @throws ParseException
	*/
	public String SetDateFormat(String myDate,String strFormat) throws ParseException
	{

	        SimpleDateFormat sdf = new SimpleDateFormat(strFormat);
	String sDate = sdf.format(sdf.parse(myDate));
	System.err.println(sDate);
	        return sDate;
	}

	    public String FormatDateTime(String strDateTime, String strFormat)
	{
	String sDateTime = strDateTime;
	try {
	Calendar Cal = parseDateTime(strDateTime);
	SimpleDateFormat sdf = null;
	sdf = new SimpleDateFormat(strFormat);
	sDateTime = sdf.format(Cal.getTime());
	} catch (Exception e) {

	        }
	return sDateTime;
	}

	    public static Calendar parseDateTime(String baseDate)
	{
	Calendar cal = null;
	cal = new GregorianCalendar();
	int yy = Integer.parseInt(baseDate.substring(0, 4));
	int mm = Integer.parseInt(baseDate.substring(5, 7)) - 1;
	int dd = Integer.parseInt(baseDate.substring(8, 10));
	int hh = 0;
	int mi = 0;
	int ss = 0;
	if(baseDate.length() > 12)
	{
	hh = Integer.parseInt(baseDate.substring(11, 13));
	mi = Integer.parseInt(baseDate.substring(14, 16));
	ss = Integer.parseInt(baseDate.substring(17, 19));
	}
	cal.set(yy, mm, dd, hh, mi, ss);
	return cal;
	}
	public int getDay(String strDate)
	{
	Calendar cal = parseDateTime(strDate);
	return  cal.get(Calendar.DATE);
	}

	    public int getMonth(String strDate)
	{
	Calendar cal = parseDateTime(strDate);

	        return cal.get(Calendar.MONTH) + 1;
	}

	    public int getWeekDay(String strDate)
	{
	Calendar cal = parseDateTime(strDate);
	return cal.get(Calendar.DAY_OF_WEEK);
	}

	    public String getWeekDayName(String strDate)
	{
	String mName[] = {
	"日", "一", "二", "三", "四", "五", "六"
	};
	int iWeek = getWeekDay(strDate);
	iWeek = iWeek - 1;
	return "星期" + mName[iWeek];
	}

	    public int getYear(String strDate)
	{
	Calendar cal = parseDateTime(strDate);
	return cal.get(Calendar.YEAR) + 1900;
	}

	    public String DateAdd(String strDate, int iCount, int iType)
	{
	Calendar Cal = parseDateTime(strDate);
	int pType = 0;
	if(iType == 0)
	{
	pType = 1;
	} else
	if(iType == 1)
	{
	pType = 2;
	} else
	if(iType == 2)
	{
	pType = 5;
	} else
	if(iType == 3)
	{
	pType = 10;
	} else
	if(iType == 4)
	{
	pType = 12;
	} else
	if(iType == 5)
	{
	pType = 13;
	}
	Cal.add(pType, iCount);
	SimpleDateFormat sdf = null;
	if(iType <= 2)
	sdf = new SimpleDateFormat("yyyy-MM-dd");
	else
	sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	String sDate = sdf.format(Cal.getTime());
	return sDate;
	}

	    public String DateAdd(String strOption, int iDays, String strStartDate)
	{
	if(!strOption.equals("d"));
	return strStartDate;
	}

	    public int DateDiff(String strDateBegin, String strDateEnd, int iType)
	{
	Calendar calBegin = parseDateTime(strDateBegin);
	Calendar calEnd = parseDateTime(strDateEnd);
	long lBegin = calBegin.getTimeInMillis();
	long lEnd = calEnd.getTimeInMillis();
	int ss = (int)((lBegin - lEnd) / 1000L);
	int min = ss / 60;
	int hour = min / 60;
	int day = hour / 24;
	if(iType == 0)
	return hour;
	if(iType == 1)
	return min;
	if(iType == 2)
	return day;
	else
	return -1;
	}

	/*****************************************
	* @功能     判断某年是否为闰年
	* @return  boolean
	* @throws ParseException
	****************************************/
	public boolean isLeapYear(int yearNum){
	boolean isLeep = false;
	/**判断是否为闰年，赋值给一标识符flag*/
	if((yearNum % 4 == 0) && (yearNum % 100 != 0)){
	isLeep = true;
	}  else if(yearNum % 400 ==0){
	isLeep = true;
	} else {
	isLeep = false;
	}
	return isLeep;
	}

	    
	/*****************************************
	* @功能     计算当前日期某年的第几周
	* @return  interger
	* @throws ParseException
	****************************************/
	public int getWeekNumOfYear(){
	Calendar calendar = Calendar.getInstance();
	int iWeekNum = calendar.get(Calendar.WEEK_OF_YEAR);
	return iWeekNum;
	}

	    /*****************************************
	* @功能     计算指定日期某年的第几周
	* @return  interger
	* @throws ParseException
	****************************************/
	public int getWeekNumOfYearDay(String strDate ) throws ParseException{
	Calendar calendar = Calendar.getInstance();
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	Date curDate = format.parse(strDate);
	calendar.setTime(curDate);
	int iWeekNum = calendar.get(Calendar.WEEK_OF_YEAR);
	return iWeekNum;
	}
	/*****************************************
	* @功能     计算某年某周的开始日期
	* @return  interger
	* @throws ParseException
	****************************************/
	public String getYearWeekFirstDay(int yearNum,int weekNum) throws ParseException {

	Calendar cal = Calendar.getInstance();
	cal.set(Calendar.YEAR, yearNum);
	cal.set(Calendar.WEEK_OF_YEAR, weekNum);
	cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
	//分别取得当前日期的年、月、日
	String tempYear = Integer.toString(yearNum);
	String tempMonth = Integer.toString(cal.get(Calendar.MONTH) + 1);
	String tempDay = Integer.toString(cal.get(Calendar.DATE));
	String tempDate = tempYear + "-" +tempMonth + "-" + tempDay;
	return SetDateFormat(tempDate,"yyyy-MM-dd");



	}
	/*****************************************
	* @功能     计算某年某周的结束日期
	* @return  interger
	* @throws ParseException
	****************************************/
	public String getYearWeekEndDay(int yearNum,int weekNum) throws ParseException {
	Calendar cal = Calendar.getInstance();
	cal.set(Calendar.YEAR, yearNum);
	cal.set(Calendar.WEEK_OF_YEAR, weekNum + 1);
	cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
	//分别取得当前日期的年、月、日
	String tempYear = Integer.toString(yearNum);
	String tempMonth = Integer.toString(cal.get(Calendar.MONTH) + 1);
	String tempDay = Integer.toString(cal.get(Calendar.DATE));
	String tempDate = tempYear + "-" +tempMonth + "-" + tempDay;
	return SetDateFormat(tempDate,"yyyy-MM-dd");
	}


	/*****************************************
	* @功能     计算某年某月的开始日期
	* @return  interger
	* @throws ParseException
	****************************************/
	public String getYearMonthFirstDay(int yearNum,int monthNum) throws ParseException {

	//分别取得当前日期的年、月、日
	String tempYear = Integer.toString(yearNum);
	String tempMonth = Integer.toString(monthNum);
	String tempDay = "1";
	String tempDate = tempYear + "-" +tempMonth + "-" + tempDay;
	return SetDateFormat(tempDate,"yyyy-MM-dd");

	}
	/*****************************************
	* @功能     计算某年某月的结束日期
	* @return  interger
	* @throws ParseException
	****************************************/
	public String getYearMonthEndDay(int yearNum,int monthNum) throws ParseException {

	       //分别取得当前日期的年、月、日
	String tempYear = Integer.toString(yearNum);
	String tempMonth = Integer.toString(monthNum);
	String tempDay = "31";
	if (tempMonth.equals("1") || tempMonth.equals("3") || tempMonth.equals("5") || tempMonth.equals("7") ||tempMonth.equals("8") || tempMonth.equals("10") ||tempMonth.equals("12")) {
	tempDay = "31";
	}
	if (tempMonth.equals("4") || tempMonth.equals("6") || tempMonth.equals("9")||tempMonth.equals("11")) {
	tempDay = "30";
	}
	if (tempMonth.equals("2")) {
	if (isLeapYear(yearNum)) {
	tempDay = "29";
	} else {
	tempDay = "28";
	}
	}
	//System.out.println("tempDay:" + tempDay);
	String tempDate = tempYear + "-" +tempMonth + "-" + tempDay;
	return SetDateFormat(tempDate,"yyyy-MM-dd");

	    }
	public static void main(String[] args) throws Exception {
		DateUtil du = new DateUtil();
		System.out.println(du.choseFormat("2007-1128",  "yyyy-MM-dd HH:mm:ss"));
		System.out.println(du.choseFormat("20071128",  "###,###"));
		System.out.println(du.getWeekDayName("2012-02-20"));
//		System.out.println(timeStamp2DateSecon("2007-1128", "yyyy-MM-dd HH:mm:ss"));
//		System.out.println(timeStamp2DateDay("2007-11211", "yyyy-MM-dd"));
//		System.out.println(timeStamp2DateSecon("2007-11-28-17-53-30", "yyyy-MM-dd HH:mm:ss"));

	}
}
