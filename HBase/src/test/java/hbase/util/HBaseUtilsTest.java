package java.hbase.util;

import com.basic.hbase.util.HBaseUtils;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * locate com.basic.hbase.util
 * Created by 79875 on 2017/5/24.
 */
public class HBaseUtilsTest {

    private Logger logger= LoggerFactory.getLogger(HBaseUtilsTest.class);

    @Test
    public void creatTable() throws Exception {
        List<Result> psn = HBaseUtils.getAllRecord("psn");
        for(Result res :psn){
            logger.info(res.toString());
        }
    }

}
