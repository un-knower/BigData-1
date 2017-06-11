package com.jusfoun.oozie.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Properties;

/**
 * Created by admin on 2016/10/17.
 */
public class OozieUtil {


    /**
     * 把参数放到对象里,提交oozie任务
     *
     * @param constants
     * @return
     * @throws OozieClientException
     */
    public static void runJob(Constants constants) throws OozieClientException {

        String jobId = null;

        OozieClient wc = new OozieClient(constants.getOozieClient());
        Properties conf = wc.createConfiguration();
        conf.setProperty("nameNode", constants.getHadoopNameNode());
        conf.setProperty("queueName", constants.getYarnQueueName());
        conf.setProperty("oozie.wf.application.path", constants.getWokflowxmlpath());
        conf.setProperty("jobTracker", constants.getHadoopJjobTracker());
        conf.setProperty("oozie.use.system.libpath", "true");

        jobId = wc.run(conf);
        System.out.println("Workflow job submitted");
        System.out.println("jobid  " + jobId);

    }

    /**
     * 把参数放到对象里,提交oozie任务,如果有参数,传入到args
     *
     * @param constants
     * @param args
     * @return
     * @throws OozieClientException
     */
    public static String runJob(Constants constants, String[] args) throws OozieClientException {

        String jobId = null;

        OozieClient wc = new OozieClient(constants.getOozieClient());
        Properties conf = wc.createConfiguration();
        conf.setProperty("nameNode", constants.getHadoopNameNode());
        conf.setProperty("queueName", constants.getYarnQueueName());
        conf.setProperty("oozie.wf.application.path", constants.getWokflowxmlpath());
        conf.setProperty("jobTracker", constants.getHadoopJjobTracker());
        conf.setProperty("oozie.use.system.libpath", "true");
        conf.setProperty("json", args[0]);

        jobId = wc.run(conf);
        System.out.println("Workflow job submitted");
        System.out.println("jobid  " + jobId);


        return jobId;
    }


    /**
     * kill 任务根据任务id
     *
     * @param jobId
     * @return
     * @throws OozieClientException
     */
    public static boolean killRunningJob(String jobId, Constants constants) throws OozieClientException {

        OozieClient wc = new OozieClient(constants.getOozieClient());
        Properties conf = wc.createConfiguration();
        conf.setProperty("nameNode", constants.getHadoopNameNode());
        conf.setProperty("queueName", constants.getYarnQueueName());
        conf.setProperty("oozie.wf.application.path", constants.getWokflowxmlpath());
        conf.setProperty("jobTracker", constants.getHadoopJjobTracker());
        conf.setProperty("oozie.use.system.libpath", "true");

        wc.kill(jobId);
        System.out.println("Workflow Job Kill Jobid  " + jobId);

        return true;
    }


    /**
     * 运行定时调度任务
     *
     * @param jsonNew
     * @return
     * @throws OozieClientException
     */
    public static String runCoordinatorTask(String jsonNew, Constants constants) throws OozieClientException {

        String json = URLEncoder.encode(jsonNew);
        String jobId = null;

        OozieClient wc = new OozieClient(constants.getOozieClient());
        Properties conf = wc.createConfiguration();
        conf.setProperty("nameNode", constants.getHadoopNameNode());
        conf.setProperty("queueName", constants.getYarnQueueName());
        conf.setProperty("jobTracker", constants.getHadoopJjobTracker());
        conf.setProperty("oozie.use.system.libpath", "true");
        conf.setProperty("json", json);


        conf.setProperty("workflowAppUri", constants.getWorkflowAppUri());
        conf.setProperty("frequency", constants.getFrequency());
        conf.setProperty("start", constants.getStart());
        conf.setProperty("end", constants.getEnd());

        conf.setProperty(wc.COORDINATOR_APP_PATH, constants.getCoordApplicationPath());
        conf.setProperty("oozie.service.coord.check.maximum.frequency", "true");

        jobId = wc.run(conf);
        String log = wc.getJobLog(jobId);
        System.out.println(log);


        System.out.println("Workflow job submitted jobid  " + jobId);

        return jobId;
    }

//    public static void main(String[] args) throws OozieClientException {
////        String json;
////        //数据库接入
//////        json = "{\"code\":\"1\",\"data\":{\"charset\":\"utf-8\",\"dbPwd\":\"5Rb!!@bqC%\",\"maxcon\":\"10\",\"uid\":\"123\",\"baseinfo\":{\"plName\":\"管道名称1\",\"createTime\":\"2016-11-07 13:58:12.0\",\"resType\":21,\"updateUid\":\"123\",\"plAddress\":\"192.168.15.15\",\"updateTime\":\"2016-11-07 13:58:12.0\",\"id\":541,\"plDesc\":\"\",\"plBtype\":11,\"createUid\":\"123\",\"plType\":1},\"dbUsername\":\"root\",\"port\":\"3306\",\"dbName\":\"db1\",\"tableList\":[{\"taskExeTime\":null,\"countTask\":null,\"tbName\":\"user\",\"taskUnit\":null,\"tbAlia\":\"别名\",\"taskJobId\":\"\",\"taskLatestExeTime\":\"\",\"uid\":\"123\",\"taskInterval\":null,\"labelId\":\"123\",\"tbDes\":\"用户表\",\"taskIntervalMinute\":\"\",\"taskStartDate\":\"\",\"dbId\":541,\"countTaskFail\":null,\"id\":420,\"labelName\":\"\",\"fieldList\":[{\"fieldAlias\":\"\",\"uid\":\"123\",\"fieldName\":\"id\",\"isPrekey\":null,\"isNull\":null,\"fieldDes\":\"主键\",\"isIncrease\":null,\"fieldLen\":null,\"id\":1324,\"fieldPrecision\":null,\"fieldType\":\"\",\"tbId\":420},{\"fieldAlias\":\"\",\"uid\":\"123\",\"fieldName\":\"name\",\"isPrekey\":null,\"isNull\":null,\"fieldDes\":\"名称\",\"isIncrease\":null,\"fieldLen\":null,\"id\":1325,\"fieldPrecision\":null,\"fieldType\":\"\",\"tbId\":420}],\"taskNewResult\":\"\",\"splitColumn\":\"\",\"countTaskSucess\":null,\"taskIsIncrement\":null},{\"taskExeTime\":null,\"countTask\":null,\"tbName\":\"user2\",\"taskUnit\":null,\"tbAlia\":\"别名2\",\"taskJobId\":\"\",\"taskLatestExeTime\":\"\",\"uid\":\"123\",\"taskInterval\":null,\"labelId\":\"123\",\"tbDes\":\"用户表2\",\"taskIntervalMinute\":\"\",\"taskStartDate\":\"\",\"dbId\":541,\"countTaskFail\":null,\"id\":421,\"labelName\":\"\",\"fieldList\":[],\"taskNewResult\":\"\",\"splitColumn\":\"\",\"countTaskSucess\":null,\"taskIsIncrement\":null}],\"isOffline\":1,\"id\":541},\"message\":\"调用成功\"}";
//////        txt 测试
////        json = "{\"code\":\"1\",\"data\":{\"endLine\":5,\"charset\":\"gbk\",\"rmBlank\":0,\"splitChar\":\"#\",\"filePath\":\"/data/warehouse/123/1478509887098.txt\",\"startLine\":1,\"sheetNum\":1,\"nameLine\":1,\"uid\":\"123\",\"baseinfo\":{\"plName\":\"plName\",\"createTime\":\"2016-11-07 17:11:29.0\",\"resType\":21,\"updateUid\":\"123\",\"plAddress\":\"\",\"updateTime\":\"2016-11-07 17:11:29.0\",\"id\":549,\"plDesc\":\"\",\"plBtype\":12,\"createUid\":\"123\",\"plType\":6},\"labelId\":\"123456\",\"id\":549,\"labelName\":\"labelName\",\"fieldList\":[{\"fieldFormat\":\"\",\"fieldIndex\":1,\"uid\":\"123\",\"fieldName\":\"name\",\"fieldLen\":10,\"id\":105,\"fieldPrecision\":null,\"fieldType\":\"String\",\"fileId\":549},{\"fieldFormat\":\"\",\"fieldIndex\":2,\"uid\":\"123\",\"fieldName\":\"age\",\"fieldLen\":null,\"id\":106,\"fieldPrecision\":null,\"fieldType\":\"int\",\"fileId\":549}],\"fileType\":1},\"message\":\"调用成功\"}";
////        //excel测试
//////        json = "{\"code\":\"1\",\"data\":{\"endLine\":5,\"charset\":\"gbk\",\"rmBlank\":0,\"splitChar\":\"#\",\"filePath\":\"/data/warehouse/123/1478511677784.xlsx\",\"startLine\":1,\"sheetNum\":1,\"nameLine\":1,\"uid\":\"123\",\"baseinfo\":{\"plName\":\"plName\",\"createTime\":\"2016-11-07 17:41:19.0\",\"resType\":21,\"updateUid\":\"123\",\"plAddress\":\"\",\"updateTime\":\"2016-11-07 17:41:19.0\",\"id\":550,\"plDesc\":\"\",\"plBtype\":12,\"createUid\":\"123\",\"plType\":6},\"labelId\":\"123456\",\"id\":550,\"labelName\":\"labelName\",\"fieldList\":[{\"fieldFormat\":\"\",\"fieldIndex\":1,\"uid\":\"123\",\"fieldName\":\"name\",\"fieldLen\":10,\"id\":107,\"fieldPrecision\":null,\"fieldType\":\"String\",\"fileId\":550},{\"fieldFormat\":\"\",\"fieldIndex\":2,\"uid\":\"123\",\"fieldName\":\"age\",\"fieldLen\":null,\"id\":108,\"fieldPrecision\":null,\"fieldType\":\"int\",\"fileId\":550}],\"fileType\":2},\"message\":\"调用成功\"}";
////        try {
////
////            runSpark( json, "/piple/data2Parquet.xml" );
////        } catch ( Exception e  ) {
////            e.printStackTrace();
////        }
//
//
//        String tableparam = "{\"table\":{\"taskExeTime\":0,\"countTask\":0,\"tbName\":\"user\",\"taskUnit\":1,\"tbAlia\":\"别名1\",\"taskJobId\":\"\",\"taskLatestExeTime\":\"\",\"uid\":\"123\",\"taskInterval\":1,\"labelId\":\"123,123\",\"tbDes\":\"\",\"taskIntervalMinute\":\"6\",\"taskStartDate\":\"2016-11-20 10:20:12.0\",\"dbId\":679,\"countTaskFail\":0,\"id\":518,\"labelName\":\"n1,n2\",\"fieldList\":[{\"fieldAlias\":\"\",\"uid\":\"123\",\"fieldName\":\"id\",\"isPrekey\":1,\"isNull\":0,\"fieldDes\":\"\",\"isIncrease\":0,\"fieldLen\":0,\"id\":1772,\"fieldPrecision\":10,\"fieldType\":\"int\",\"tbId\":518},{\"fieldAlias\":\"\",\"uid\":\"123\",\"fieldName\":\"name\",\"isPrekey\":0,\"isNull\":1,\"fieldDes\":\"名称\",\"isIncrease\":0,\"fieldLen\":255,\"id\":1773,\"fieldPrecision\":0,\"fieldType\":\"varchar\",\"tbId\":518},{\"fieldAlias\":\"\",\"uid\":\"123\",\"fieldName\":\"create_date\",\"isPrekey\":0,\"isNull\":1,\"fieldDes\":\"\",\"isIncrease\":0,\"fieldLen\":50,\"id\":1774,\"fieldPrecision\":0,\"fieldType\":\"varchar\",\"tbId\":518},{\"fieldAlias\":\"\",\"uid\":\"123\",\"fieldName\":\"create_date2\",\"isPrekey\":0,\"isNull\":1,\"fieldDes\":\"\",\"isIncrease\":0,\"fieldLen\":0,\"id\":1775,\"fieldPrecision\":0,\"fieldType\":\"date\",\"tbId\":518},{\"fieldAlias\":\"\",\"uid\":\"123\",\"fieldName\":\"create_date3\",\"isPrekey\":0,\"isNull\":1,\"fieldDes\":\"\",\"isIncrease\":0,\"fieldLen\":0,\"id\":1776,\"fieldPrecision\":0,\"fieldType\":\"datetime\",\"tbId\":518}],\"taskNewResult\":\"\",\"splitColumn\":\"id\",\"countTaskSucess\":0,\"taskIsIncrement\":0},\"db\":{\"charset\":\"\",\"dbPwd\":\"5Rb!!@bqC%\",\"maxcon\":\"\",\"uid\":\"123\",\"baseinfo\":{\"plName\":\"管道名称1\",\"createTime\":\"2016-11-10 10:07:59.0\",\"resType\":22,\"updateUid\":\"123\",\"plAddress\":\"192.168.15.15\",\"updateTime\":\"2016-11-10 10:07:59.0\",\"id\":679,\"plDesc\":\"\",\"plBtype\":11,\"createUid\":\"123\",\"plType\":1},\"dbUsername\":\"root\",\"port\":\"3306\",\"dbName\":\"db1\",\"tableList\":[],\"isOffline\":0,\"id\":679}}" ;
//        OOzieEntity entity = new OOzieEntity();
//        entity.setStart("2016-11-10 14:16:12" );
//        entity.setCoordApplicationPath( "/workflow/xml/sourceManagerCoordinator.xml" );
//        entity.setFrequency(  "6");
//        entity.setWorkflowAppUri("/workflow/xml/sourceManager.xml");
////        String jobId = OozieUtil.runCoordinatorTask( tableparam ,entity ) ;
////        System.out.print( jobId );
//
//        OozieUtil.killRunSparkJob("0000438-160908101317232-oozie-oozi-W", "/workflow/xml/sourceManager.xml" ) ;
//
//
//    }

}
