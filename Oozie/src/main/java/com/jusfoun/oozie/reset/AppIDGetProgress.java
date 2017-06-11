package com.jusfoun.oozie.reset;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;




public class AppIDGetProgress {

    public String AppIDGetProgressFunction(String appID) throws InterruptedException {

//        调用封装好了的http方法
        JavaNetURLRestFulClientFunction function = new JavaNetURLRestFulClientFunction();
        String json = function.RestStringToJson("http://ubt202:8088/proxy/application_1464571591429_0034/ws/v1/mapreduce/jobs/job_1464571591429_0034/tasks");
        System.out.println(json);
        JSONObject jsApp = JSON.parseObject(json);
//        System.out.println(jsApp.get("app"));
        Object app = jsApp.get("app");
        JSONObject js = JSON.parseObject(app.toString());

        System.out.println("当前job执行进度 "+js.get("progress"));
        String progress = js.get("progress").toString();
        return progress;

    }
}
