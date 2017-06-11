package com.hushiwei.learn;

/**
 * Created by hushiwei on 17-2-10.
 */
public class App {
    public static void main(String[] args) {
        String forward = "SUBMIT  1  LVBV4J0B2AJ063987  FORWARD  {VID:111,VTYPE:3,TIME:20150720151510,FLAG:1,TYPE:1,RESULT:1}";
        byte[] bytes = forward.getBytes();
        for (byte aByte : bytes) {
            System.out.println(aByte);
        }
    }
}
