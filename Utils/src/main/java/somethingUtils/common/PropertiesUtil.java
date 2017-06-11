package somethingUtils.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Created by HuShiwei on 2016/10/19 0019.
 */
public class PropertiesUtil implements Serializable{
    private static final long serialVersionUID = 1L;
    public static Properties properties;
    public static String host;
    private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    static {
        InputStream in;
        try {
//            配置文件放在resource路径下即可
            in = PropertiesUtil.class.getResourceAsStream("/conf.properties");
//            获取配置文件的路径
//            PropertiesUtil.class.getClassLoader().getResource("ipdb.dat").getPath();

            properties = new Properties();
            properties.load(in);
            initConfig();
            System.out.println("PropertiesUtil初始化完毕");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 初始化全局配置
    public static void initConfig() {
        host = properties.getProperty("host");
    }

}
