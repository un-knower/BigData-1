package com.jusfoun.oozie.oozie;

/**
 * Created by HuShiwei on 2016/11/17 0017.
 */
public class Constants {
    private String OozieClient;
    private String HadoopNameNode;
    private String YarnQueueName;
    private String HadoopJjobTracker;
    private String WorkflowAppUri;
    private String Frequency;
    private String Start;
    private String End;
    private String CoordApplicationPath;
    private String wokflowxmlpath;

    public Constants(String oozieClient, String hadoopNameNode, String yarnQueueName, String hadoopJjobTracker, String wokflowxmlpath) {
        OozieClient = oozieClient;
        HadoopNameNode = hadoopNameNode;
        YarnQueueName = yarnQueueName;
        HadoopJjobTracker = hadoopJjobTracker;
        this.wokflowxmlpath = wokflowxmlpath;
    }

    public Constants(String oozieClient, String hadoopNameNode, String yarnQueueName, String hadoopJjobTracker, String workflowAppUri, String frequency, String start, String end, String coordApplicationPath, String wokflowxmlpath) {
        OozieClient = oozieClient;
        HadoopNameNode = hadoopNameNode;
        YarnQueueName = yarnQueueName;
        HadoopJjobTracker = hadoopJjobTracker;
        WorkflowAppUri = workflowAppUri;
        Frequency = frequency;
        Start = start;
        End = end;
        CoordApplicationPath = coordApplicationPath;
        this.wokflowxmlpath = wokflowxmlpath;
    }

    public String getOozieClient() {
        return OozieClient;
    }

    public void setOozieClient(String oozieClient) {
        OozieClient = oozieClient;
    }

    public String getHadoopNameNode() {
        return HadoopNameNode;
    }

    public void setHadoopNameNode(String hadoopNameNode) {
        HadoopNameNode = hadoopNameNode;
    }

    public String getYarnQueueName() {
        return YarnQueueName;
    }

    public void setYarnQueueName(String yarnQueueName) {
        YarnQueueName = yarnQueueName;
    }

    public String getHadoopJjobTracker() {
        return HadoopJjobTracker;
    }

    public void setHadoopJjobTracker(String hadoopJjobTracker) {
        HadoopJjobTracker = hadoopJjobTracker;
    }

    public String getWorkflowAppUri() {
        return WorkflowAppUri;
    }

    public void setWorkflowAppUri(String workflowAppUri) {
        WorkflowAppUri = workflowAppUri;
    }

    public String getFrequency() {
        return Frequency;
    }

    public void setFrequency(String frequency) {
        Frequency = frequency;
    }

    public String getStart() {
        return Start;
    }

    public void setStart(String start) {
        Start = start;
    }

    public String getEnd() {
        return End;
    }

    public void setEnd(String end) {
        End = end;
    }

    public String getCoordApplicationPath() {
        return CoordApplicationPath;
    }

    public void setCoordApplicationPath(String coordApplicationPath) {
        CoordApplicationPath = coordApplicationPath;
    }

    public String getWokflowxmlpath() {
        return wokflowxmlpath;
    }

    public void setWokflowxmlpath(String wokflowxmlpath) {
        this.wokflowxmlpath = wokflowxmlpath;
    }
}
