package cn.flysheep.kafka.bean;

import cn.flysheep.kafka.util.MsgCounter;
import org.softee.management.annotation.Description;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;

import java.util.Map;

/**
 * JMX 查看信息
 * Created by yanglunyi on 2017/7/11.
 */
@MBean
@Description("kafka monitor")
public class StatusBean {

    @ManagedAttribute
    @Description("kafka消息计数")
    public Map<String, String> getKafkaMsgCountMap() {
        return MsgCounter.getCountMap();
    }
}
