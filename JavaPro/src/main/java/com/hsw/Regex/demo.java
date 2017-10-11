package com.hsw.Regex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hushiwei on 2017/10/10.
 * desc :
 */
public class demo {

  public static final String regex = "??aaff??";

  public static void main(String[] args) throws IOException {
    Pattern pattern = Pattern.compile(regex);
    ArrayList<String> lines  = FileUtil.readlines("/Users/hushiwei/IdeaProjects/BigData/JavaPro/src/resources/adxreq.txt");
    for (String line : lines) {
      System.out.println(line);

      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        String group = matcher.group();
        System.out.println(group);
      }
    }
    }
  }

