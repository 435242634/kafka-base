package cn.flysheep.kafka.core;

/**
 * Created by yanglunyi on 2017/7/11.
 */
public interface IKConsumerAction {
    void process(String message);
}
