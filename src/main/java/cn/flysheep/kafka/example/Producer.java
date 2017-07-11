package cn.flysheep.kafka.example;

import cn.flysheep.kafka.core.KProducer;
import com.alibaba.fastjson.JSONObject;

/**
 * Created by yanglunyi on 2017/7/11.
 */
public class Producer {

    public static void main(String[] args) throws InterruptedException {
        JSONObject object = new JSONObject();
        object.put("bootstrap.servers", "192.168.10.221:9093");
        // kafka topic(添加flysheep.前缀是和kafka已有配置区分，在传给KafkaProducer类前会移除掉)
        object.put("flysheep.topic.id", "test1");
        // kafka生产者对象个数 增加个数一定程度可以加快消息发送速度
        object.put("flysheep.producer.num", "2");

        KProducer producer = new KProducer(object.toJSONString());
        int count = 0;
        while (true) {
            ++count;
            String msg = "msg-" + count;
            producer.send(msg);
            try {
                Thread.sleep(Integer.MAX_VALUE);
            } catch (Exception e) {

            }
            System.out.println("sendmsg, " + msg);
        }
    }
}
