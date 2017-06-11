package com.hushiwei.flume.exception;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */
//无效的消息异常
public class InvalidMessageException extends Exception{
    private static final long serialVersionUID = 1L;

    public InvalidMessageException(String message)
    {
        super(message);
    }
}
