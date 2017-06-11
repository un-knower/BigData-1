package com.hushiwei.learn;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * Created by hushiwei on 17-2-10.
 */
public class MySick extends AbstractSink implements Configurable {
    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        return null;
    }

    @Override
    public void configure(Context context) {

    }
}
