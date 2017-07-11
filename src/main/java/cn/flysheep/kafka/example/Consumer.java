package cn.flysheep.kafka.example;

import cn.flysheep.kafka.core.IKConsumerAction;
import cn.flysheep.kafka.core.KConsumer;
import com.alibaba.fastjson.JSONObject;

/**
 * Created by yanglunyi on 2017/7/11.
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException {
        JSONObject object = new JSONObject();
        object.put("bootstrap.servers", "192.168.10.221:9093");
        object.put("group.id", "testgroup");
        // kafka topic(添加flysheep.前缀是和kafka已有配置区分，在传给KafkaConsumer类前会移除掉)
        object.put("flysheep.topic.id", "test1");
        // 消费速度限制 比如每秒100条记录
        object.put("flysheep.ratelimit.sec", "100");
        // 消息有效期 过期的消息不再处理 单位ms
        object.put("flysheep.retention.ms", "10000");

        KConsumer consumer = new KConsumer(object.toJSONString(), new IKConsumerAction() {
            public void process(String message) {
                System.out.println(message);
            }
        });

        Thread.sleep(Integer.MAX_VALUE);
    }
}
