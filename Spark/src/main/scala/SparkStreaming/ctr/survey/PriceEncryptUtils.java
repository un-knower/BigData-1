package SparkStreaming.ctr.survey;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;

/**
 * 价格加密工具
 */
public class PriceEncryptUtils {

    public static void main(String... args) throws Exception {
        String pri = encodePrice("45749f8b79ccb54e982ce59ac1151ef0", "45749f8b79ccb54e982ce59ac1151ef0", 89.9d);
        double dd = decodePrice("45749f8b79ccb54e982ce59ac1151ef0", "45749f8b79ccb54e982ce59ac1151ef0",pri);
        System.out.print(dd);
    }

    /**
     * 按照规则对加密的密文解密。解密步骤如下：
     * <p>
     * 1、首先对加密的价格进行base64解码，结果应为长度 28 字节的数据数据。<br>
     * 格式如下:iv(16) | encrypted_price (8) | integrity(4) 。<br>
     * 2、用dec_key对iv(16)进行"HmacSHA1"加密得到混淆码de_byte。<br>
     * 3、用de_byte的前八位和八位加密价格数组Pencrpt(8)进行异或运算的到真正的价格数组。<br>
     * 5、将价格数组按照小字节节序转换成{@code Double}类型的价格。<br>
     * 6、将dec_price数组和iv数组合并成一个24位的数组，然后用完整性签名的密钥进行加密，<br>
     * 得到一个完整性校验数组，取其前四位和integrity(4)，进行比较，如果相等，返回解密的价格，<br>
     * 如果两个数组不同，则抛出异常"signature is illegal"
     * </p>
     *
     * @param dec_key 加密的密钥
     * @param i_key   完整性校验的密钥
     * @param e_enc   原始密文
     * @return 返回解密后的价格
     * @throws
     */
    public static double decodePrice(String dec_key, String i_key, String e_enc) throws Exception {
        byte[] e_src = Base64.decodeBase64(e_enc); // base64解码
        byte[] iv = Arrays.copyOf(e_src, 16); // 初始化矢量16位数组
        byte[] en_price = Arrays.copyOfRange(e_src, 16, 24); // 加密的价格数组
        byte[] integrity = Arrays.copyOfRange(e_src, 24, 28); // 完整性签名的前4位
        byte[] de_byte = HMACUtils.hmacSHA1(Hex.decodeHex(dec_key.toCharArray()), iv); // 加密得到价格的加密码
        byte[] dec_price = xor_bytes(en_price, de_byte, 8); // 通过异或运算得到价格数组

        Double price = BinaryConvertUtils.convertByteToDoubleL(dec_price); // 把价格数组按照小字节序转换成价格

        byte[] o_ikey = HMACUtils.hmacSHA1(Hex.decodeHex(i_key.toCharArray()), // 将价格数组和时间戳数组，合成一个24位的数组
                ArrayUtils.addAll(dec_price, iv)); // 加密得到原始完整性校验数组
        boolean flag = ArrayUtils.isEquals(integrity, Arrays.copyOf(o_ikey, 4)); // 比较两个签名数组是否相同
        if (!flag) {
            throw new Exception("signature is illegal"); // 返回签名非法的错误
        }
        return price;
    }

    /**
     * 1、先得到当前时间戳，然后按照小字节序转化成数组 time_stamp。<br>
     * 2、将time_stamp经过MD5加密得到初始化矢量数组 iv。 <br>
     * 3、用加密密钥和iv数组进行HMAC.SHA1加密运算，得到加密数组 en_byte。<br>
     * 4、拿到价格数组 price。<br>
     * 5、通过价格数组price与加密数组en_byte的前八位进行异或运算（xor_bytes方法）得到加密后的价格enc_price。<br>
     * 6、将price与iv连接组成24位的数组然后与i_key进行HMAC运算得到完整性签名o_ikey。<br>
     * 7、取o_ikey的前四位作为完整性签名使用，得到sign。<br>
     * 8、将iv拼接enc_price拼接sign得到一个28位的数组e_src。<br>
     * 9、将e_src经过网络安全的Base64加密得到真正的加密的价格e_enc。<br>
     *
     * @param enc_key        加密的密钥
     * @param i_key          完整性校验的密钥
     * @param original_price 原始价格
     * @return 返回加密后的价格
     * @throws
     */
    public static String encodePrice(String enc_key, String i_key, double original_price) throws Exception {
        byte[] time_stamp = BinaryConvertUtils.convertLongToByteL(DateTimeUtils.getCurrentSecond()); // 得到时间戳的byte数组
        byte[] iv = MD5Utils.md5(time_stamp); // 对时间戳的byte数组加密，得到16位的初始化矢量数组
        byte[] en_byte = HMACUtils.hmacSHA1(Hex.decodeHex(enc_key.toCharArray()), iv); // 用加密密钥和初始化矢量数据计算出加密数组
        byte[] price = BinaryConvertUtils.convertDoubleToByteL(original_price); // 得到价格数组

        byte[] enc_price = xor_bytes(price, en_byte, 8); // 加密数组的前8位和价格的前八位做异或操作

        byte[] o_ikey = HMACUtils.hmacSHA1(Hex.decodeHex(i_key.toCharArray()), // 将价格数组和初始化矢量数组，合成一个24位的数组
                ArrayUtils.addAll(price, iv)); // 加密得到原始完整性签名数组
        byte[] sign = ArrayUtils.subarray(o_ikey, 0, 4); // 取完整性签名的前4位使用
        byte[] e_src = ArrayUtils.addAll(ArrayUtils.addAll(iv, enc_price), sign); // 将初始化矢量、价格、完整性签名拼接
        String e_enc = Base64.encodeBase64URLSafeString(e_src); // 用网络安全的加密算法进行
        // base64加密
        return e_enc;
    }

    /****************************************************
     * 私有方法
     ****************************************************/

    /**
     * 两个数组按位进行异或操作，得到一个新的数组
     *
     * @param bytes1 第一个数组
     * @param bytes2 第二个数组
     * @param length 数组长度
     * @return 返回操作后新的数组
     */
    private static byte[] xor_bytes(byte[] bytes1, byte[] bytes2, int length) {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = (byte) (bytes1[i] ^ bytes2[i]);
        }
        return result;
    }

}
