package com.hushiwei.mr.GdPro.utils;

import com.hushiwei.mr.GdPro.mr.TimeUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class JsoupUtils {

    public static String getJsoup(String str) {
        /**
         * stbNum、date、e、s、p、sn、e-s
         */
        String stbNum = null;
        String date = null;
        String e = null;
        String s = null;
        String p = null;
        String sn = null;
        int ess = 0;
        int sss = 0;
//		String str = "<GHApp><WIC cardNum='1370083461' stbNum='03091011120286570' date='2012-09-24' pageWidgetVersion='1.0'><A e='01:42:09' s='01:37:09' n='104' t='1' pi='909' p='%E9%BB%84%E9%A3%9E%E9%B8%BF%E4%B9%8B%E9%93%81%E9%B8%A1%E6%96%97%E8%9C%88%E8%9A%A3' sn='BTV影视' /></WIC></GHApp>";
        Document doc1 = Jsoup.parse(str);
        if (doc1 != null) {
            Element wic = doc1.select("WIC").first();
            if (wic != null) {
                stbNum = wic.attr("stbNum");
                date = wic.attr("date");
            }
            Element a = doc1.select("A").first();
            if (a != null) {
                e = a.attr("e");
                ess = TimeUtil.getSeconds(e);
                s = a.attr("s");
                sss = TimeUtil.getSeconds(s);
                p = a.attr("p");
                try {
                    p = URLDecoder.decode(p, "utf-8");
                } catch (UnsupportedEncodingException e1) {
                    e1.printStackTrace();
                }
                int index = p.indexOf("(");
                if (index != -1) {
                    p = p.substring(0, index);
                }
                sn = a.attr("sn");
            }
        }
        return stbNum + "@" + date + "@" + e + "@" + s + "@" + p + "@" + sn + "@" + (ess - sss);
//		System.out.println("stbNum="+stbNum+"@date="+date+"@e="+e+"@s="+s+"@p="+p+"@sn="+sn+"@"+(ess-sss));
//		System.out.println(stbNum+"@"+date+"@"+e+"@"+s+"@"+p+"@"+sn+"@"+(ess-sss));
    }
}
