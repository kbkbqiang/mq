package io.github.kbkbqiang.core;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author zhaoqiang
 * @Description
 * @date 2022/11/22 18:32
 */
public interface RocketMQListener<T> {

    ConsumeConcurrentlyStatus consumerMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext);

}
