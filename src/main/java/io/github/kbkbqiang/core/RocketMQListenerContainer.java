package io.github.kbkbqiang.core;

import org.springframework.beans.factory.DisposableBean;

/**
 * @author zhaoqiang
 * @Description
 * @date 2022/11/22 18:30
 */
public interface RocketMQListenerContainer extends DisposableBean {

    void setupMessageListener(RocketMQListener messageListener);

}
