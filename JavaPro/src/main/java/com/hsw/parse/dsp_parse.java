package com.hsw.parse;

/**
 * Created by hushiwei on 2017/9/1.
 * desc :
 */
public class dsp_parse extends dsp_parsecommom {

  @Override
  public void parse(String line) {
    System.out.println(regex+"bean1");
  }

  public static void main(String[] args) {
    ParseLogImp parse = new dsp_parse1();

    parse.parse("");
  }
}
