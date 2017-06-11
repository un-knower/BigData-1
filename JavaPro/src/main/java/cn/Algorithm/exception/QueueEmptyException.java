package cn.Algorithm.exception;

/**
 * Created by HuShiwei on 2016/10/18 0018.
 */
public class QueueEmptyException extends RuntimeException {
    public QueueEmptyException(String err) {
        super(err);
    }
}
