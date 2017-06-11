package cn.Algorithm.exception;

/**
 * Created by HuShiwei on 2016/10/17 0017.
 */
//线性表中出现序号越界时[抛出该异常
public class OutOfBoundaryException extends RuntimeException {
    public OutOfBoundaryException(String err) {
        super(err);
    }
}
