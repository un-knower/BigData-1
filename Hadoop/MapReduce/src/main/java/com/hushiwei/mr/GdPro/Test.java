package com.hushiwei.mr.GdPro;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class Test {
	public static void main(String[] args){
		String html = "<p>An <a href='http://www.jb51.net/'><b>www.jb51.net</b></a> link.</p>";
		Document doc = Jsoup.parse(html);//解析HTML字符串返回一个Document实现
		Element link = doc.select("a").first();//查找第一个a元素
		String text = doc.body().text(); // "An www.jb51.net link"//取得字符串中的文本
		String linkHref = link.attr("href"); // "http://www.jb51.net/"//取得链接地址
//		System.out.println(linkHref);
		String linkText = link.text(); // "www.jb51.net""//取得链接地址中的文本
		String linkOuterH = link.outerHtml(); 
		    // "<a href="http://www.jb51.net"><b>www.jb51.net</b></a>"
		String linkInnerH = link.html(); // "<b>www.jb51.net</b>"//取得链接内的html内容
		
		/**
		 * stbNum、date、e、s、p、sn、e-s
		 */
		String str = "<GHApp><WIC cardNum='1370083461' stbNum='03091011120286570' date='2012-09-24' pageWidgetVersion='1.0'><A e='01:42:09' s='01:37:09' n='104' t='1' pi='909' p='%E9%BB%84%E9%A3%9E%E9%B8%BF%E4%B9%8B%E9%93%81%E9%B8%A1%E6%96%97%E8%9C%88%E8%9A%A3' sn='BTV影视' /></WIC></GHApp>";
		Document doc1 = Jsoup.parse(str);
		Element wic = doc1.select("WIC").first();
		String stbNum = wic.attr("stbNum");
		String date = wic.attr("date");
		Element a = doc1.select("A").first();
		String e = a.attr("e");
		String[] es = e.split(":");
		int ess = Integer.parseInt(es[0])*3600+Integer.parseInt(es[1])*60+Integer.parseInt(es[2]);
		String s = a.attr("s");
		String[] ss = s.split(":");
		int sss = Integer.parseInt(ss[0])*3600+Integer.parseInt(ss[1])*60+Integer.parseInt(ss[2]);
		String p = a.attr("p");
		String sn = a.attr("sn");
		System.out.println("stbNum="+stbNum+"@date="+date+"@e="+e+"@s="+s+"@p="+p+"@sn="+sn+"@"+(ess-sss));
	}
}
