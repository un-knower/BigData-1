package SparkStreaming.ctr.survey;

import org.apache.commons.codec.binary.Hex;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * {@code HMAC}加密工具类
 */
public final class HMACUtils {

    private static String SHA1 = "HmacSHA1"; //
    private static String MD5 = "HmacMD5"; //

    /**
     * 用{@code key}对{@code content}进行SHA1加密
     * 
     * @param key 用来加密用的秘钥，类型是{@link byte}数组
     * @param content 要加密的内容，类型是{@link byte}数组
     * @return 返回加密后的{@link String}字符串
     * @throws Exception 抛出异常
     */
    public static String hmacSHA1AsString(byte[] key, byte[] content) throws Exception {
        Mac mac = getMac(key, SHA1);
        return Hex.encodeHexString(mac.doFinal(content));
    }

    /**
     * 用{@code key}对{@code content}进行SHA1加密
     * 
     * @param key 用来加密用的秘钥，类型是{@link byte}数组
     * @param content 要加密的内容，类型是{@link byte}数组
     * @return 返回加密后的{@link byte}数组
     * @throws Exception 抛出异常
     */
    public static byte[] hmacSHA1(byte[] key, byte[] content) throws Exception {
        Mac mac = getMac(key, SHA1);
        return mac.doFinal(content);
    }

    /**
     * 用{@code key}对{@code content}进行MD5加密
     * 
     * @param key 用来加密用的秘钥，类型是{@link byte}数组
     * @param content 要加密的内容，类型是{@link byte}数组
     * @return 返回加密后的{@link String}字符串
     * @throws Exception 抛出异常
     */
    public static String hmacMD5AsString(byte[] key, byte[] content) throws Exception {
        Mac mac = getMac(key, MD5);
        return Hex.encodeHexString(mac.doFinal(content));
    }

    /**
     * 用{@code key}对{@code content}进行MD5加密
     * 
     * @param key 用来加密用的秘钥，类型是{@link byte}数组
     * @param content 要加密的内容，类型是{@link byte}数组
     * @return 返回加密后的{@link byte}数组
     * @throws Exception 抛出异常
     */
    public static byte[] hmacMD5(byte[] key, byte[] content) throws Exception {
        Mac mac = getMac(key, MD5);
        return mac.doFinal(content);
    }

    /*****************************************************
     * 以下为私有方法
     *****************************************************/

    /**
     * 根据给的{@code key} 和 {@code algorithm} 初始化得到 {@link Mac} 对象
     *
     * @param key 加密数据用的，{@code key}类型
     * @param algorithm 加密算的名称，{@link String}类型
     * @return 返回 {@link Mac}实例
     * @throws Exception 抛出异常
     */
    private static Mac getMac(byte[] key, String algorithm) throws Exception {
        SecretKey sKey = new SecretKeySpec(key, algorithm);
        Mac mac = Mac.getInstance(algorithm);
        mac.init(sKey);
        return mac;
    }
}
