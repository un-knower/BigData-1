package com.jusfoun.oozie.reset;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Created by admin on 2016/4/19.
 */
public class OozieIDGetAppID {

    public String OozieIDGetAppIDFunction(String oozieID) throws InterruptedException {

        JavaNetURLRestFulClientFunction function = new JavaNetURLRestFulClientFunction();
        String json = function.RestStringToJson("http://ubt204:11000/oozie/v2/job/"+oozieID);
//        System.out.println(json);
        JSONObject jsonObject = JSON.parseObject(json);
        String appID = jsonObject.getJSONArray("actions").getJSONObject(1).get("consoleUrl").toString().replaceAll("(.*)/proxy/", "").replace("/", "");
        String appStatus = jsonObject.getJSONArray("actions").getJSONObject(1).get("status").toString();
        System.out.println("=======当前job执行进度========");
//        System.out.println(appID);
        System.out.println("当前job执行状态 "+appStatus);
        return appID;
    }
}
