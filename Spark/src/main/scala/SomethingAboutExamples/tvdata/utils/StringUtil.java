package SomethingAboutExamples.tvdata.utils;

/**
 * Created by HuShiwei on 2016/9/8 0008.
 */
public class StringUtil {
    public static long strTime2Long(String time) {
        final String[] split = time.split(":");
        final long longTime = str2int(split[0]) * 60 * 60 * 1000 + str2int(split[1]) * 60 * 1000 + str2int(split[2]) * 1000;
        return longTime;

    }

    public static long str2int(String str) {
        return Long.parseLong(str);
    }

    public static long spendTime(String startTime, String entTime) {
        long spentTime = strTime2Long(entTime) - strTime2Long(startTime);
        if (spentTime < 0) {
            final long l = strTime2Long(entTime) + 86400000 - strTime2Long(startTime);
            spentTime = l;
        }
        return spentTime / 1000;
    }

    public static void main(String[] args) {
        String startTime = "23:59:00";
        String entTime = "00:01:00";
        final long spendTime = spendTime(startTime, entTime);
        System.out.println(spendTime);


    }
}
