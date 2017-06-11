package com.jusfoun.oozie;

import com.jusfoun.oozie.reset.AppIDGetProgress;
import com.jusfoun.oozie.reset.OozieIDGetAppID;

/**
 * Created by admin on 2016/4/20.
 */
public class TestOozie {
    public static void main(String[] args) throws InterruptedException {

        String sql = "{\"input\":\"/examples/src/main/resources/spark/custom3.txt\",\"output\":\"hdfs://Jusfoun2016:8020/examples/clean/out/1\",\"cleanCol\":{\"1\":{\"200\":\"1000\"},\"4\":{\"TX\":\"newTX\"}},\"separator\":\",\",\"fieldsObjects\":[{\"index\":0,\"name\":\"id\",\"type\":\"string\"},{\"index\":3,\"name\":\"qh\",\"type\":\"string\"},{\"index\":4,\"name\":\"code\",\"type\":\"string\"}]}";
/*        String jobId = "0000082-160419160742625-oozie-oozi-W";
        OozieIDGetAppID oozieIDGetAppID = new OozieIDGetAppID();
        AppIDGetProgress appIDGetProgress = new AppIDGetProgress();
        String appid = oozieIDGetAppID.OozieIDGetAppIDFunction(jobId);
        String progress = appIDGetProgress.AppIDGetProgressFunction(appid);*/
        while (true) {
            String jobId = "00000010-160426132903478-oozie-oozi-W";
            OozieIDGetAppID oozieIDGetAppID =  new OozieIDGetAppID();
            AppIDGetProgress appIDGetProgress = new AppIDGetProgress();
            String appid = oozieIDGetAppID.OozieIDGetAppIDFunction(jobId);
            String progress = appIDGetProgress.AppIDGetProgressFunction(appid);
        }
    }
}
