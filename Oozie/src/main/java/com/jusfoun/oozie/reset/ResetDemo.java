package com.jusfoun.oozie.reset;

/**
 * Created by HuShiwei on 2016/5/31 0031.
 */
public class ResetDemo {
    public static void main(String[] args) {
        JavaNetURLRestFulClientFunction function = new JavaNetURLRestFulClientFunction();
        String json = null;
        try {
            json = function.RestStringToJson("http://192.168.4.212:8989/msb/cpy/list");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(json);
    }
}
