package com.hsw.simple;

import com.hsw.Regex.FileUtil;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Test;

/**
 * Created by hushiwei on 2017/10/10.
 * desc :
 *
 *
 * 匹配中文: [一-龥]
 * 匹配字母或数字或下划线: /w
 * 匹配标点符号: [\pP]
 */
public class regex {

  public static String checkLog(String str) {
    // 替换hive默认分隔符/001
    String log = str.replaceAll("\\u0001", "");
    System.out.println("替换hive默认分隔符"+log);
    // 匹配中文,英文,符号
    String regEx = "[\u4E00-\u9FA5]|\\w|\\pP|\\pZ|\\pC|\\pN|\\pS";
//    String regEx = "[\u4E00-\u9FA5]|\\w|[\\p{P}&&[^\\x01]]|\\pZ|\\pC|\\pN|\\pS";  //|\\pC&&[^\\x01]]
    Pattern p = Pattern.compile(regEx);
    Matcher m = p.matcher(log);
    String replaceLog = m.replaceAll("").trim();
    System.out.println("替换正常字符后剩下的为: "+replaceLog);
    System.out.println("剩下的字符长度为: "+replaceLog.length());

    if (replaceLog.length() > 0) {
      return "";
    } else {
    return log;
    }
  }

  @Test
  public void regexlog() throws Exception {
    ArrayList<String> lines  = FileUtil.readlines("/Users/hushiwei/IdeaProjects/BigData/JavaPro/src/resources/adxreq.txt");
    for (String line : lines) {
      System.out.println("原始字符串: "+line);
      String tmp = checkLog(line);
      System.out.println("可用日志为: "+tmp);
      System.out.println("------------------------------------------------------------");
    }
  }

  @Test
  public void test1() throws Exception {
    String   str   =   "*adCVs*34_a _09_b5*[/435^*&城池()^$$&*).{}+.|.)%%*(*.中国vendoréè}34{45[]12.fd'*&999下面是中文的字符￥……{}【】。，；’“‘”？";
    System.out.println(str);
    System.out.println(checkLog(str));
  }

  @Test
  public void matchChinese() throws Exception {
    // 要匹配的字符串
    String source = "<span title='5 星级酒店' class='dx dx5'>";
    // 将上面要匹配的字符串转换成小写
    // source = source.toLowerCase();
    // 匹配的字符串的正则表达式
    String reg_charset = "<span[^>]*?title=\'([0-9]*[\\s|\\S]*[\u4E00-\u9FA5]*)\'[\\s|\\S]*class=\'[a-z]*[\\s|\\S]*[a-z]*[0-9]*\'";

    Pattern p = Pattern.compile(reg_charset);
    Matcher m = p.matcher(source);
    while (m.find()) {
      System.out.println(m.group(1));
    }

  }


}
