package cn.Algorithm.exception;

/**
 * Created by HuShiwei on 2016/10/17 0017.
 */
public class InvalidNodeException extends RuntimeException {
    public InvalidNodeException(String err) {
        super(err);
    }
}
