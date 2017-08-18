package SomethingAboutExamples.sparkStreaming.ctr.survey;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 二进制数据转换类
 */
public final class BinaryConvertUtils {

    /**
     * 按大端字节序将{@code int}转换成{@code byte}数组
     * 
     * @param i 需要转换的数值
     * @return 返回{@code byte}数组
     */
    public static byte[] convertIntToByteB(int i) {
        byte[] bytes = new byte[4];
        bytes[3] = (byte) (i & 0xff);
        bytes[2] = (byte) ((i >> 8) & 0xff);
        bytes[1] = (byte) ((i >> 16) & 0xff);
        bytes[0] = (byte) ((i >>> 24) & 0xff);
        return bytes;
    }

    /**
     * 按小端字节序将{@code int}转换成{@code byte}数组
     * 
     * @param i 需要转换的数值
     * @return 返回{@code byte}数组
     */
    public static byte[] convertIntToByteL(int i) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (i & 0xff);
        bytes[1] = (byte) ((i >> 8) & 0xff);
        bytes[2] = (byte) ((i >> 16) & 0xff);
        bytes[3] = (byte) ((i >>> 24) & 0xff);
        return bytes;
    }

    /**
     * 按大端字节序将{@code byte}数组转换成{@code int}，数组长度应为4位
     * 
     * @param bytes 需要转换的{@code byte}数组
     * @return 返回{@code int}数值
     */
    public static int convertByteToIntB(byte[] bytes) {
        return (bytes[3] & 0xff) | ((bytes[2] & 0xff) << 8) | ((bytes[1] & 0xff) << 16) | ((bytes[0] & 0xff) << 24);
    }

    /**
     * 按小端字节序将{@code byte}数组转换成{@code int}，数组长度应为4位
     * 
     * @param bytes 需要转换的{@code byte}数组
     * @return 返回{@code int}数值
     */
    public static int convertByteToIntL(byte[] bytes) {
        return (bytes[0] & 0xff) | ((bytes[1] & 0xff) << 8) | ((bytes[2] & 0xff) << 16) | ((bytes[3] & 0xff) << 24);
    }

    /**
     * 按大端字节序将{@code long}转换成{@code byte}数组
     * 
     * @param l 需要转换的值
     * @return 返回{@code byte}数组
     */
    public static byte[] convertLongToByteB(long l) {
        byte[] bytes = new byte[8];
        bytes[7] = (byte) (l & 0xff);
        bytes[6] = (byte) ((l >> 8) & 0xff);
        bytes[5] = (byte) ((l >> 16) & 0xff);
        bytes[4] = (byte) ((l >> 24) & 0xff);
        bytes[3] = (byte) ((l >> 32) & 0xff);
        bytes[2] = (byte) ((l >> 40) & 0xff);
        bytes[1] = (byte) ((l >> 48) & 0xff);
        bytes[0] = (byte) ((l >>> 56) & 0xff);
        return bytes;
    }

    /**
     * 按小端字节序将{@code long}转换成{@code byte}数组
     * 
     * @param l 需要转换的值
     * @return 返回{@code byte}数组
     */
    public static byte[] convertLongToByteL(long l) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) (l & 0xff);
        bytes[1] = (byte) ((l >> 8) & 0xff);
        bytes[2] = (byte) ((l >> 16) & 0xff);
        bytes[3] = (byte) ((l >> 24) & 0xff);
        bytes[4] = (byte) ((l >> 32) & 0xff);
        bytes[5] = (byte) ((l >> 40) & 0xff);
        bytes[6] = (byte) ((l >> 48) & 0xff);
        bytes[7] = (byte) ((l >>> 56) & 0xff);
        return bytes;
    }

    /**
     * 按大端字节序将{@code byte}数组转换成{@code long}，数组长度应为8位
     * 
     * @param bytes 需要转换的{@code byte}数组
     * @return 返回{@code long}数值
     */
    public static long convertByteToLongB(byte[] bytes) {
        return (bytes[7] & 0xff) | ((bytes[6] & 0xff) << 8) | ((bytes[5] & 0xff) << 16) | ((bytes[4] & 0xff) << 24)
                | ((bytes[3] & 0xff) << 32) | ((bytes[2] & 0xff) << 40) | ((bytes[1] & 0xff) << 48)
                | ((bytes[0] & 0xff) << 56);
    }

    /**
     * 按小端字节序将{@code byte}数组转换成{@code long}，数组长度应为8位
     * 
     * @param bytes 需要转换的{@code byte}数组
     * @return 返回{@code long}数值
     */
    public static long convertByteToLongL(byte[] bytes) {
        return (bytes[0] & 0xff) | ((bytes[1] & 0xff) << 8) | ((bytes[2] & 0xff) << 16) | ((bytes[3] & 0xff) << 24)
                | ((bytes[4] & 0xff) << 32) | ((bytes[5] & 0xff) << 40) | ((bytes[6] & 0xff) << 48)
                | ((bytes[7] & 0xff) << 56);
    }

    /**
     * 按大端字节序将{@code double}转换成{@code byte}数组
     * 
     * @param d 需要转换的值
     * @return 返回{@code byte}数组
     */
    public static byte[] convertDoubleToByteB(double d) {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(d).array();
        return bytes;
    }

    /**
     * 按小端字节序将{@code double}转换成{@code byte}数组
     * 
     * @param d 需要转换的值
     * @return 返回{@code byte}数组
     */
    public static byte[] convertDoubleToByteL(double d) {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(d).array();
        return bytes;
    }

    /**
     * 按大端字节序将{@code byte}数组转换成{@code double}，数组长度应为8位
     * 
     * @param bytes 需要转换的{@code byte}数组
     * @return 返回{@code double}值
     */
    public static double convertByteToDoubleB(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    /**
     * 按小端字节序将{@code byte}数组转换成{@code double}，数组长度应为8位
     * 
     * @param bytes 需要转换的{@code byte}数组
     * @return 返回{@code double}值
     */
    public static double convertByteToDoubleL(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
    }
}
