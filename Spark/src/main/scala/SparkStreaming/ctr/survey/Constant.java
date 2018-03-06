package SparkStreaming.ctr.survey;

/**
 * 存放常量
 *
 */
public class Constant {

    /** 在{@code redis}中存储的{@code DSP}信息的{@code key}的前缀 **/
    public static String DSPLISTKEY = "adx_dsplist";

    /** 在{@code redis}中存储的{@code creative}的信息的{@code key}的前缀 **/
    public static String CREATIVEPREFIXKEY = "adx_creative_";

    /** 在{@code redis}中存储的{@code DSP}的{@code QPS}数的{{@code key}的前缀 **/
    public static String QPS = "qps_";

    /** {@code ADX}向{@code DSP}发送广播的过期时间 **/
    public static long TIMEOUT;

    /** 存储{@code ADX}的曝光监播地址 **/
    public static String ADX_IMPURL;

    /** 存储{@code ADX}的点击曝光地址 **/
    public static String ADX_CLKURL;

    /** 设置字符集 **/
    public static String CHARSET = "UTF-8";
}
