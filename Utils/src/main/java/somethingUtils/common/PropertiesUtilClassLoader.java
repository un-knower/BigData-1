package somethingUtils.common;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by HuShiwei on 2016/10/19 0019.
 */
public class PropertiesUtilClassLoader implements Serializable{
    private static final long serialVersionUID = 1L;
    public static Properties properties;
    public static String quorm;
    private static Logger logger = LoggerFactory.getLogger(PropertiesUtilClassLoader.class);

    static {
        InputStream in;
        try {
//            配置文件放在resource路径下即可
            properties = new Properties();
            properties.load(ClassLoader.getSystemResourceAsStream("conf.properties"));
            initConfig();
            System.out.println("PropertiesUtil初始化完毕");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 初始化全局配置
    public static void initConfig() {
        quorm = properties.getProperty("hbase.zookeeper.quorum");
    }

    public static void main(String[] args) {
        System.out.println(quorm);
    }

}
