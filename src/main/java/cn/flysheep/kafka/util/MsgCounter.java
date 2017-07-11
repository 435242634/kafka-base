package cn.flysheep.kafka.util;

import cn.flysheep.kafka.bean.MsgCountBean;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 生产者生产消息的计数器
 * Created by yanglunyi on 2017/7/11.
 */
public class MsgCounter {

    // Map<topic, count>
    private final static Map<String, MsgCountBean> msgCountMap = new ConcurrentHashMap<String, MsgCountBean>();

    public enum Type {
        SEND, SENDSUCC, SENDFAIL, RECV
    }

    public static void increment(String topic, Type type) {
        MsgCountBean bean = msgCountMap.get(topic);
        if (bean == null) {
            synchronized (MsgCounter.class) {
                if (bean == null) {
                    bean = new MsgCountBean();
                    msgCountMap.put(topic, bean);
                }
            }
        }

        switch (type) {
            case SEND:
                bean.incrSend();
                break;
            case SENDSUCC:
                bean.incrSendSucc();
                break;
            case SENDFAIL:
                bean.incrSendFail();
                break;
            case RECV:
                bean.incrRecv();
                break;
        }
    }

    public static Map<String, String> getCountMap() {
        Map<String, String> map = new HashMap<String, String>();
        for (String topic : msgCountMap.keySet()) {
            MsgCountBean bean = msgCountMap.get(topic);
            if (bean != null) {
                map.put(topic, bean.toString());
            }
        }
        return map;
    }
}
