package com.jusfoun.oozie.oozie;

import org.apache.oozie.client.OozieClient;

import java.util.Properties;

public class RunOozie {

	public static void main(String args[]) throws Exception {

        String filepath = "Oozie/conf/appSpark.properties";
        PropertiesUtil pu = PropertiesUtil.getInstance(filepath);
        String OozieClient = pu.GetValueByKey("OozieClient");
        String nameNode = pu.GetValueByKey("nameNode");
        String path = pu.GetValueByKey("path");
        String jobTracker = pu.GetValueByKey("jobTracker");
        String queueName = pu.GetValueByKey("queueName");

        Constants constants = new Constants(OozieClient,nameNode,queueName,jobTracker,path);

        OozieUtil.runJob(constants);
//        OozieUtil.killRunningJob("0000001-161117133915453-oozie-oozi-W", constants);
    }

}
