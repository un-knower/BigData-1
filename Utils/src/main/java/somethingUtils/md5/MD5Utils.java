package somethingUtils.md5;

import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;

/**
 *
 */
public final class MD5Utils {
	
	/**
	 * MD5 加密
	 * @param content 需要加密的数据是{@link String}类型的字符串
	 * @return 返回{@link String}类型的字符串
	 * @throws Exception 抛出异常
	 */
	public static String md5AsString(String content) throws Exception {
		MessageDigest mDigest = MessageDigest.getInstance("MD5");
		return Hex.encodeHexString(mDigest.digest(content.getBytes("UTF-8"))); 
	}
	
	/**
	 * MD5 加密
	 * @param content 需要加密的数据是{@link byte}类型数组
	 * @return 返回{@link String}类型的字符串
	 * @throws Exception 
	 */
	public static String md5AsString(byte[] content) throws Exception {
		MessageDigest mDigest = MessageDigest.getInstance("MD5");
		return Hex.encodeHexString(mDigest.digest(content));
	}
	
	/**
	 * MD5 加密
	 * @param content 需要加密的数据是{@link String}类型的数据
	 * @return 返回{@link byte}类型的数组
	 * @throws Exception 抛出异常
	 */
	public static byte[] md5(String content) throws Exception {
		MessageDigest mDigest = MessageDigest.getInstance("MD5");
		return mDigest.digest(content.getBytes("UTF-8")); 
	}
	
	/**
	 * MD5 加密
	 * @param content 需要加密的数据是{@link byte}类型数组
	 * @return 返回{@link byte}类型的数组
	 */
	public static byte[] md5(byte[] content) throws Exception {
		MessageDigest mDigest = MessageDigest.getInstance("MD5");
		return mDigest.digest(content); 
	}

	public static void main(String[] args) {
		String imei = "352514062501634";
		try {

			String md5AsString = md5AsString(imei);
			System.out.println(md5AsString);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
