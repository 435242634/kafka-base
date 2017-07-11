package cn.flysheep.kafka.bean;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 消息计数
 * Created by yanglunyi on 2017/7/11.
 */
public class MsgCountBean {

    /**
     * 发送总数
     */
    private AtomicLong send = new AtomicLong();

    /**
     * 发送成功数
     */
    private AtomicLong sendSucc = new AtomicLong();

    /**
     * 发送失败数
     */
    private AtomicLong sendFail = new AtomicLong();

    /**
     * 接受总数
     */
    private AtomicLong recv = new AtomicLong();


    public void incrSend() {
        this.send.incrementAndGet();
    }

    public void incrSendSucc() {
        this.sendSucc.incrementAndGet();
    }

    public void incrSendFail() {
        this.sendFail.incrementAndGet();
    }

    public void incrRecv() {
        this.recv.incrementAndGet();
    }

    @Override
    public String toString() {
        return "{" +
                "send=" + send.get() +
                ", sendSucc=" + sendSucc.get() +
                ", sendFail=" + sendFail.get() +
                ", recv=" + recv.get() +
                '}';
    }
}
