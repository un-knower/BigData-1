package com.hushiwei.kafka;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
import java.util.Random;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

public class MyPartitioner
        implements Partitioner
{
    private static Logger logger = Logger.getLogger(MyPartitioner.class);
    private Random random = new Random();

    public MyPartitioner(VerifiableProperties props) { logger.info("初始化自定义分区选择器");
    }

    public int partition(Object key, int numPartitions)
    {
        int partition = 0;
        try {
            if (key == null)
            {
                partition = this.random.nextInt(numPartitions);
            } else {
                byte[] vid = (byte[])key;

                partition = Math.abs(new String(vid, "UTF-8").hashCode()) % numPartitions;
                logger.info(new String(vid, "UTF-8") + ":" + partition);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return partition;
    }
}
