package io.github.kbkbqiang.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.RPCHook;
import io.github.kbkbqiang.properties.RocketMQProperties;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * @author zhaoqiang
 * @Description
 * @date 2022/11/22 18:35
 */
@Slf4j
public class DefaultRocketMQListenerContainer implements InitializingBean, RocketMQListenerContainer {

    @Setter
    @Getter
    private ObjectMapper objectMapper = new ObjectMapper();

    @Setter
    @Getter
    private boolean started;

    @Setter
    @Getter
    private String nameSrvAddr;

    @Setter
    @Getter
    private String topic;

    @Setter
    @Getter
    private String tag = "*";

    @Setter
    private String accessKey;

    @Setter
    private String secretKey;

    //消费者分组名称
    @Setter
    private String consumerGroup;

    private DefaultMQPushConsumer defaultMQPushConsumer;

    @Setter
    private RocketMQListener rocketMQListener;

    @Setter
    private RocketMQProperties rocketMQProperties;

    @Override
    public void setupMessageListener(RocketMQListener<?> messageListener) {
        rocketMQListener = messageListener;
    }

    public synchronized void start() throws MQClientException {
        if (this.isStarted()) {
            throw new IllegalStateException("container already started. " + this.toString());
        }

        initMQPushConsumer();

        this.setStarted(true);

        log.info("started container: {}", this.toString());

    }


    private void initMQPushConsumer() throws MQClientException {
        Assert.notNull(nameSrvAddr, "Property 'nameSrvAddr' is required");
        Assert.notNull(rocketMQListener, "Property 'rocketMQListener' is required");
        Assert.notNull(topic, "Property 'topic' is required");
        Assert.notNull(accessKey, "Property 'accessKey' is required");
        Assert.notNull(secretKey, "Property 'secretKey' is required");
        Assert.notNull(consumerGroup, "Property 'consumerGroup' is required");

        defaultMQPushConsumer = new DefaultMQPushConsumer(consumerGroup, getAclRPCHook(), new AllocateMessageQueueAveragely());
        defaultMQPushConsumer.setNamesrvAddr(getNameSrvAddr());
        defaultMQPushConsumer.subscribe(getTopic(), tag);

        /**
         * CONSUME_FROM_LAST_OFFSET 默认策略，从该队列最尾开始消费，跳过历史消息
         * CONSUME_FROM_FIRST_OFFSET 从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
         */
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        defaultMQPushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            try {
                long now = System.currentTimeMillis();
                rocketMQListener.consumerMessage(msgs, context);
                long costTime = System.currentTimeMillis() - now;
                log.debug("consume getAckIndex {} cost: {} ms", context.getAckIndex(), costTime);
            } catch (Exception e) {
                log.error("rocketmq consumer is error", e);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            //ACK机制，消费成功
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        defaultMQPushConsumer.start();
    }

    public RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

    @Override
    public void destroy() throws Exception {

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    @Override
    public String toString() {
        return "DefaultRocketMQListenerContainer{" +
                "nameSrvAddr='" + nameSrvAddr + '\'' +
                ", topic='" + topic + '\'' +
                ", tag='" + tag + '\'' +
                ", consumerGroup='" + consumerGroup + '\'' +
                '}';
    }
}
