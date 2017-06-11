package somethingUtils.common;

import java.io.*;
import java.util.Enumeration;
import java.util.Properties;

//关于Properties类常用的操作
public class PropertiesUtils {
    private static PropertiesUtils prop;
    private String configPath;

    private PropertiesUtils(String configPath) {
        this.configPath = configPath;
    }

    /**
     * 传入配置文件的路径，获取一个配置文件工具实例
     * @param configPath
     * @return
     */
    public static PropertiesUtils getInstance(String configPath) {
        if (prop == null) {
            prop = new PropertiesUtils(configPath);
            return prop;
        }
        return prop;
    }


    /**
     * 根据Key读取Value
     *
     * @param key
     * @return
     */
    public String GetValueByKey(String key) {
        Properties pps = new Properties();
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(configPath));
            pps.load(in);
            String value = pps.getProperty(key);
            return value;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 读取Properties的全部信息
     *
     * @throws IOException
     */
    public void GetAllProperties() throws IOException {
        Properties pps = new Properties();
        InputStream in = new BufferedInputStream(new FileInputStream(configPath));
        pps.load(in);
        Enumeration en = pps.propertyNames(); //得到配置文件的名字

        while (en.hasMoreElements()) {
            String strKey = (String) en.nextElement();
            String strValue = pps.getProperty(strKey);
            System.out.println(strKey + " = " + strValue);
        }

    }


    /**
     * 写入Properties信息
     *
     * @param pKey
     * @param pValue
     * @throws IOException
     */
    public void WriteProperties(String pKey, String pValue) throws IOException {
        Properties pps = new Properties();

        InputStream in = new FileInputStream(configPath);
        //从输入流中读取属性列表（键和元素对）
        pps.load(in);
        //调用 Hashtable 的方法 put。使用 getProperty 方法提供并行性。
        //强制要求为属性的键和值使用字符串。返回值是 Hashtable 调用 put 的结果。
        OutputStream out = new FileOutputStream(configPath);
        pps.setProperty(pKey, pValue);
        //以适合使用 load 方法加载到 Properties 表中的格式，
        //将此 Properties 表中的属性列表（键和元素对）写入输出流
        pps.store(out, "Update " + pKey + " name");
    }
}