package cn.flysheep.kafka.core;

import cn.flysheep.kafka.util.AutoRun;
import cn.flysheep.kafka.util.MsgCounter;
import cn.flysheep.kafka.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 消息生产封装类
 * Created by yanglunyi on 2017/7/11.
 */
public class KProducer extends AbstractBase {
    private final Logger logger = LoggerFactory.getLogger(KProducer.class);

    private final static AtomicLong selectSeq = new AtomicLong();

    static {
        AutoRun.run();
    }

    private final List<KafkaProducer<String, String>> producers = new ArrayList<KafkaProducer<String, String>>();
    private final String topic;
    private final int produceNum;
    private final String keyPrefix;
    private final AtomicLong seq = new AtomicLong();

    /**
     * 构造kafka生产类实例
     *
     * @param json kafka生产者属性
     */
    public KProducer(String json) {
        Properties props = Util.json2Props(json);
        this.topic = (String) props.remove("flysheep.topic.id");
        String num = StringUtils.defaultIfEmpty((String) props.remove("flysheep.producer.num"), "1");
        this.produceNum = Integer.valueOf(num);

        // 校验props是否合法
        Util.checkUndefined(ProducerConfig.configNames(), props);

        // 合并配置
        Properties newProps = new Properties();
        newProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        newProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        newProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "cn.flysheep.kafka.core.SimplePartition");
        newProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        newProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        Util.mergeProps(newProps, props);

        String myName = System.getProperty("myName");
        keyPrefix = String.format("%s-%s-%s-%d-", myName == null ? "default" : myName, topic,
                DateFormatUtils.format(new Date(), "MMddHHmmss"), instanceSeq.getAndIncrement()).toLowerCase();

        // 构造生产者，可以构造多个生产者加快发送速度
        for (int i = 0; i < this.produceNum; ++i) {
            newProps.put(ProducerConfig.CLIENT_ID_CONFIG, getClientId());
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(newProps);
            producers.add(producer);
        }
    }

    /**
     * 发送消息给kafka
     *
     * @param message 发送的消息
     */
    public void send(final String message) {
        final String key = getMsgKey();
        MsgCounter.increment(topic, MsgCounter.Type.SEND);
        logger.debug("send,key:{},value:{}", key, message);
        getProducer().send(new ProducerRecord<String, String>(topic, key, message), new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (metadata == null) {
                    // 发送失败做重发??
                    logger.error("send failed,key:{},value:{}", key, message, e);
                    MsgCounter.increment(topic, MsgCounter.Type.SENDFAIL);
                } else {
                    // 发送成功
                    MsgCounter.increment(topic, MsgCounter.Type.SENDSUCC);
                    logger.debug("send succ,partition:{},offset:{},key:{},value:{}", metadata.partition(),
                            metadata.offset(), key, message);
                }
            }
        });
    }

    /**
     * 获取消息的唯一标识码
     *
     * @return
     */
    private String getMsgKey() {
        return keyPrefix + seq.getAndIncrement();
    }

    /**
     * 顺序选择多个生产者中的一个
     *
     * @return
     */
    private KafkaProducer<String, String> getProducer() {
        return producers.get(Math.abs((int) (selectSeq.incrementAndGet() % this.produceNum)));
    }
}
