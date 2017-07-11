package cn.flysheep.kafka.core;

import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by yanglunyi on 2017/7/11.
 */
public class AbstractBase {

    protected static final AtomicLong instanceSeq = new AtomicLong();

    protected String getClientId() {
        String clientId = System.getProperty("myName");
        if (StringUtils.isBlank(clientId)) {
            clientId = this.getClass().getSimpleName();
        }
        return clientId + "-" + instanceSeq.incrementAndGet();
    }
}
