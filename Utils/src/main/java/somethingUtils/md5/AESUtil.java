package somethingUtils.md5;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * AES 算法工具类。
 */
public final class AESUtil {
    
	private static final String PRIVATE_KEY = "huanju@game";
	
	private static final String IV = "2011121211143000";
	
	
	
    /** private constructor. */
    private AESUtil() {
    }
    
    /** the name of the transformation to create a cipher for. */
    private static final String TRANSFORMATION = "AES/CBC/PKCS5Padding";
    //private static final String DETRANSFORMATION = "AES/CBC/PKCS7Padding";

    /** 算法名称 */
    private static final String ALGORITHM_NAME = "AES";
    
    public static String encrypt(String uid, String text) throws Exception {
    	
    	// 固定前缀PRIVATE_KEY + uid
    	String key = PRIVATE_KEY + uid;
    	
    	// 转化为16位key
    	key = getMD5(key);
		int size = key.length();
		key = key.substring(size / 2);
		
		// AES加密
    	byte[] data = encrypt(IV, key, text.getBytes());
    	
    	// base64转化
    	String encText = Base64.encode(data);
    	
    	return encText;
    }
    
    public static String decrypt(String uid, String text) throws Exception {
    	
    	// base64解码
    	byte[] decrypted = Base64.decode(text.getBytes());
    			
    	// 固定前缀PRIVATE_KEY + uid
    	String key = PRIVATE_KEY + uid;
    	
    	// 转化为16位key
    	key = getMD5(key);
		int size = key.length();
		key = key.substring(size / 2);
		
		// 解密
    	byte[] data = decrypt(IV, key, decrypted);
    	
    	String realText = new String(data);
    	return realText;
    }
    
    /**
     * aes 加密，AES/CBC/PKCS5Padding
     * 
     * @param key
     *            密钥字符串, 此处使用AES-128-CBC加密模式，key需要为16位
     * @param content
     *            要加密的内容
     * @param cbcIv
     *            初始化向量(CBC模式必须使用) 使用CBC模式，需要一个向量iv，可增加加密算法的强度
     * @return 加密后原始二进制字符串
     * @throws Exception
     *             Exception
     */
    public static byte[] encrypt(String cbcIv, String key, byte[] content) throws Exception {

        SecretKeySpec sksSpec = new SecretKeySpec(key.getBytes(),
                ALGORITHM_NAME);

        Cipher cipher = Cipher.getInstance(TRANSFORMATION);

        IvParameterSpec iv = new IvParameterSpec(cbcIv.getBytes());
        
        cipher.init(Cipher.ENCRYPT_MODE, sksSpec, iv);

        byte[] encrypted = cipher.doFinal(content);

        return encrypted;
    }

    /**
     * aes 解密，AES/CBC/PKCS5Padding
     * 
     * @param key
     *            密钥, 此处使用AES-128-CBC加密模式，key需要为16位
     * @param encrypted
     *            密文
     * @param cbcIv
     *            初始化向量(CBC模式必须使用) 使用CBC模式，需要一个向量iv，可增加加密算法的强度
     * @return 明文
     * @throws Exception
     *             异常
     */
    public static byte[] decrypt(String cbcIv, String key, byte[] encrypted) throws Exception {

        SecretKeySpec skeSpect = new SecretKeySpec(key.getBytes(),
                ALGORITHM_NAME);

        Cipher cipher = Cipher.getInstance(TRANSFORMATION);

        IvParameterSpec iv = new IvParameterSpec(cbcIv.getBytes());
        
        cipher.init(Cipher.DECRYPT_MODE, skeSpect, iv);
        
        byte[] decrypted = null;
		try {
			decrypted = cipher.doFinal(encrypted);
		} catch (Exception e) {
			System.out.println("error.............");
			e.printStackTrace();
		}

        return decrypted;
    }
    
    private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5',
		'6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    
    private static String getMD5(String originalString)
			throws NoSuchAlgorithmException {

		final int CONSTANT_NUMBER0XF0 = 0xf0;
		final int CONSTANT_NUMBER0X0F = 0x0f;
		final int CONSTANT_NUMBER4 = 4;

		MessageDigest digester = MessageDigest.getInstance("MD5");
		digester.update(originalString.getBytes());
		byte[] b = digester.digest();

		StringBuilder builder = new StringBuilder(b.length * 2);
		for (int i = 0; i < b.length; i++) {
			builder.append(HEX_DIGITS[(b[i] & CONSTANT_NUMBER0XF0) >>> CONSTANT_NUMBER4]);
			builder.append(HEX_DIGITS[b[i] & CONSTANT_NUMBER0X0F]);
		}
		return builder.toString();
	}
    
}
