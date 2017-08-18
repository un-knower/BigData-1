package SomethingAboutExamples.sparkStreaming.ctr.survey;

/**
 * 时间工具类
 */
public final class DateTimeUtils {

    /**
     * 拿到当前时间戳，精确到毫秒
     * 
     * @return 返回精确到毫秒秒的时间戳
     */
    public static Long getCurrentMillis() {
        return System.currentTimeMillis();
    }

    /**
     * 拿到当前时间戳，精确到秒
     * 
     * @return 返回精确到秒的时间戳
     */
    public static Long getCurrentSecond() {
        return System.currentTimeMillis() / 1000;
    }

}
