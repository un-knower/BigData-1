package com.jusfoun.oozie.oozie;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * Created by admin on 2016/4/27.
 */
public class Decoder {

    public static void main(String[] args) throws UnsupportedEncodingException {

        //测试成功的例子
//        String json = "{\"datasourceArr\":[{\"datable\":\"company\",\"password\":\"Jusfoun@2016$\",\"tableType\":\"mysql\",\"tablename\":\"company\",\"url\":\"jdbc:mysql://192.168.4.213:3306/company_no_del\",\"user\":\"bigdata\"},{\"datable\":\"company_news\",\"password\":\"Jusfoun@2016$\",\"tableType\":\"mysql\",\"tablename\":\"company_news\",\"url\":\"jdbc:mysql://192.168.4.213:3306/company_no_del\",\"user\":\"bigdata\"}],\"output\":{\"tableSaveType\":\"mysql\",\"tablename\":\"zkwTest123\",\"url\":\"jdbc:mysql://192.168.4.213:3306/sparkSQL\",\"user\":\"bigdata\",\"password\":\"Jusfoun@2016$\"},\"sql\":\" select  company.entname , company.area  from company inner Join company_news on company.ent_id = company_news.ent_id\"}";
//                String json="{\"datasourceArr\":[{\"tableType\":\"txt\",\"tablename\":\"custom\",\"txtInfo\":{\"url\":\"/examples/clean/custom1.txt\",\"separator\":\",\",\"fieldsObjects\":[{\"index\":0,\"name\":\"id\",\"type\":\"int\"},{\"index\":3,\"name\":\"qh\",\"type\":\"string\"},{\"index\":4,\"name\":\"code\",\"type\":\"double\"}]}}],\"output\":{\"tableSaveType\":\"mysql\",\"tablename\":\"txtTestTable\",\"url\":\"jdbc:mysql://192.168.4.213:3306/sparkSQL\",\"user\":\"bigdata\",\"password\":\"Jusfoun@2016$\"},\"sql\":\" select * from custom\"}";

        String json = "{\"input\":[{\"type\":\"json\",\"uid\":\"dataBase_123_572c1f4ae4c4\",\"value\":{\"sourceid\":\"1047\",\"tableName\":\"company_news\",\"fieldsObjects\":[\"ent_id\",\"news_title\",\"entname\",\"origin\",\"datetime\"],\"ip\":\"192.168.15.15\",\"username\":\"root\",\"pwd\":\"5Rb!!@bqC%\",\"dbType\":\"Mysql\",\"port\":\"3306\",\"dbName\":\"company_no_del\",\"maxConn\":\"10\",\"dataStatus\":\"1\",\"encoding\":\"UTF-8\"},\"serviceType\":\"SQL_DATA_SOURCE\"},{\"type\":\"json\",\"uid\":\"api_123_58ab28d9f991\",\"value\":{\"sourceid\":\"1048\",\"tableName\":\"企业基本信息\",\"dataStatus\":\"1\",\"apiurl\":\"http://192.168.15.15:8989/msb/cpy/list\",\"apiMethod\":\"GET\",\"params\":[{\"name\":\"p\",\"type\":\"常量\",\"value\":\"1\"}]},\"serviceType\":\"API_DATA_SOURCE\"}],\"output\":[{\"type\":\"hdfs\",\"value\":\"/123/5ff30682bc0448969ba58dc05fd3ebaa/dataFuse_123_2a428dca2c3e\",\"serviceType\":\"DATA_FUSION\"}],\"params\":{\"from\":{\"joinType\":\"inner join\",\"tables\":[{\"dataSourceNodeId\":\"api_123_58ab28d9f991\",\"fields\":[{\"byname\":\"\",\"dataSourceNodeId\":\"api_123_58ab28d9f991\",\"fieldName\":\"entId\",\"opra\":\"\",\"opraFun\":\"\",\"type\":\"String\",\"value\":\"\",\"valueFun\":\"\"}],\"tableName\":\"企业基本信息\"},{\"dataSourceNodeId\":\"dataBase_123_572c1f4ae4c4\",\"fields\":[{\"byname\":\"\",\"dataSourceNodeId\":\"dataBase_123_572c1f4ae4c4\",\"fieldName\":\"ent_id\",\"opra\":\"\",\"opraFun\":\"\",\"type\":\"varchar\",\"value\":\"\",\"valueFun\":\"\"}],\"tableName\":\"company_news\"}]},\"groupBy\":[],\"orderBy\":{\"orderBy\":[],\"orderByType\":\"\"},\"select\":[{\"byname\":\"\",\"dataSourceNodeId\":\"api_123_58ab28d9f991\",\"fieldName\":\"entId\",\"opraFun\":\"\",\"type\":\"String\",\"valueFun\":\"\"}],\"where\":[],\"tableName\":\"dataFuse1471421796031\"},\"name\":{\"userid\":\"123\",\"flowid\":\"5ff30682bc0448969ba58dc05fd3ebaa\",\"actionid\":\"dataFuse_123_2a428dca2c3e\",\"jobid\":\"${wf:id()}\"}}";
//%7B%22input%22%3A%5B%7B%22type%22%3A%22hdfs%22%2C%22value%22%3A%22%2F9fc59872109ac694%2F65579572670b4d4c88b666dabd14e109%2FdataFuse_9fc59872109ac694_ec6abc016378%22%7D%5D%2C%22output%22%3A%5B%7B%22type%22%3A%22hdfs%22%2C%22value%22%3A%22%2F9fc59872109ac694%2F65579572670b4d4c88b666dabd14e109%2Fmodel_9fc59872109ac694_43c1cacab1a9%22%2C%22serviceType%22%3A%22R_DATA_FUSION%22%7D%5D%2C%22params%22%3A%7B%22tableName%22%3A%5B%22dataFuse1469010654950%22%5D%2C%22sourceid%22%3A%221%22%2C%22sql%22%3A%22%22%2C%22args%22%3A%5B%5D%2C%22result%22%3A%22mytable_freq%22%2C%22modelPath%22%3A%22%2Fexamples%2Flex%2Fr%2Fscript%2FmarkModel.R%22%2C%22language%22%3A%22r%22%2C%22resultType%22%3A%22numeric%22%7D%2C%22name%22%3A%7B%22userid%22%3A%229fc59872109ac694%22%2C%22flowid%22%3A%2265579572670b4d4c88b666dabd14e109%22%2C%22actionid%22%3A%22model_9fc59872109ac694_43c1cacab1a9%22%2C%22jobid%22%3A%22%24%7Bwf%3Aid%28%29%7D%22%7D%7D

        String str = "%7B%22input%22%3A%5B%7B%22type%22%3A%22hdfs%22%2C%22value%22%3A%22%2F9fc59872109ac694%2F65579572670b4d4c88b666dabd14e109%2FdataFuse_9fc59872109ac694_ec6abc016378%22%7D%5D%2C%22output%22%3A%5B%7B%22type%22%3A%22hdfs%22%2C%22value%22%3A%22%2F9fc59872109ac694%2F65579572670b4d4c88b666dabd14e109%2Fmodel_9fc59872109ac694_43c1cacab1a9%22%2C%22serviceType%22%3A%22R_DATA_FUSION%22%7D%5D%2C%22params%22%3A%7B%22tableName%22%3A%5B%22dataFuse1469010654950%22%5D%2C%22sourceid%22%3A%221%22%2C%22sql%22%3A%22%22%2C%22args%22%3A%5B%5D%2C%22result%22%3A%22mytable_freq%22%2C%22modelPath%22%3A%22%2Fexamples%2Flex%2Fr%2Fscript%2FmarkModel.R%22%2C%22language%22%3A%22r%22%2C%22resultType%22%3A%22numeric%22%7D%2C%22name%22%3A%7B%22userid%22%3A%229fc59872109ac694%22%2C%22flowid%22%3A%2265579572670b4d4c88b666dabd14e109%22%2C%22actionid%22%3A%22model_9fc59872109ac694_43c1cacab1a9%22%2C%22jobid%22%3A%22%24%7Bwf%3Aid%28%29%7D%22%7D%7D";
        String encode = URLEncoder.encode(json, "utf-8");
        System.out.println(encode);

/*        String decode = URLDecoder.decode(encode, "utf-8");
        System.out.println(decode);

        String decode1 = URLDecoder.decode(str, "utf-8");
        System.out.println(decode1);*/


    }
}
