package cn.flysheep.kafka.util;

import cn.flysheep.kafka.bean.StatusBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softee.management.helper.MBeanRegistration;

import javax.management.ObjectName;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 向 JMX 注册 MBean
 * Created by yanglunyi on 2017/7/11.
 */
public class AutoRun {

    private static final Logger logger = LoggerFactory.getLogger(AutoRun.class);

    private static final AtomicBoolean loaded = new AtomicBoolean();

    public static synchronized void run() {
        try {
            if (!loaded.compareAndSet(false, true)) {
                return;
            }
            StatusBean bean = new StatusBean();
            new MBeanRegistration(bean, new ObjectName("kafka.monitor:name=status")).register();
        } catch (Throwable ex) {
            logger.error("autorun error", ex);
        }
    }
}
