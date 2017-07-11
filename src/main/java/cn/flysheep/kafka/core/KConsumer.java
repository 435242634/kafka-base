package cn.flysheep.kafka.core;

import cn.flysheep.kafka.util.AutoRun;
import cn.flysheep.kafka.util.MsgCounter;
import cn.flysheep.kafka.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * kafka消息消费封装类
 * Created by yanglunyi on 2017/7/11.
 */
public class KConsumer extends AbstractBase {
    private static final Logger logger = LoggerFactory.getLogger(KConsumer.class);

    static {
        AutoRun.run();
    }

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final int rateLimit;
    private final int retentionMs;
    private final IKConsumerAction action;

    private final AtomicLong msgCount = new AtomicLong();

    /**
     * 构造kafka消息实例
     *
     * @param json   kafka消费属性
     * @param action 消费线程得到消息后实际处理类
     */
    public KConsumer(String json, final IKConsumerAction action) {
        Properties props = Util.json2Props(json);
        this.topic = (String) props.remove("flysheep.topic.id");
        String limit = StringUtils.defaultIfEmpty((String) props.remove("flysheep.ratelimit.sec"), "-1");
        this.rateLimit = Integer.valueOf(limit);
        String retention = StringUtils.defaultIfEmpty((String) props.remove("flysheep.retention.ms"), "-1");
        this.retentionMs = Integer.valueOf(retention);

        // 校验props是否合法
        Util.checkUndefined(ConsumerConfig.configNames(), props);

        // 合并配置
        Properties newProps = new Properties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, getClientId());
        Util.mergeProps(newProps, props);

        this.consumer = new KafkaConsumer<String, String>(props);
        this.action = action;

        consumer.subscribe(Collections.singletonList(this.topic));

        // 启动单独线程消费kafka消息
        Thread t = new Thread(new Runnable() {
            public void run() {
                long start = System.currentTimeMillis();
                while (true) {
                    try {
                        ConsumerRecords<String, String> records = consumer.poll(1000);
                        for (ConsumerRecord<String, String> record : records) {
                            try {
                                MsgCounter.increment(topic, MsgCounter.Type.RECV);
                                logger.debug("received,partition:{},offset:{},key:{},value:{}",
                                        record.partition(), record.offset(), record.key(), record.value());
                                // 消息有效期检查
                                if (retentionMs > 0 && System.currentTimeMillis() - record.timestamp() > retentionMs) {
                                    logger.error("expired,partition:{},offset:{},key:{},value:{}",
                                            record.partition(), record.offset(), record.key(), record.value());
                                    continue;
                                }

                                // 提交给业务类处理
                                action.process(record.value());
                                long inc = msgCount.incrementAndGet();

                                // 限速控制
                                if (rateLimit > 0 && inc % rateLimit == 0) {
                                    long mid = System.currentTimeMillis() - start;
                                    if (mid < 1000) {
                                        Thread.sleep(1000 - mid);
                                    }
                                    start = System.currentTimeMillis();
                                }
                            } catch (Throwable ex) {
                                logger.error("consumer process error, record:{}", record, ex);
                            }
                        }
                    } catch (Throwable ex) {
                        logger.error("consumer error", ex);
                    }
                }
            }
        }, "kconsumer-" + topic.toLowerCase() + "-" + instanceSeq.getAndIncrement());

        t.start();
    }

}
