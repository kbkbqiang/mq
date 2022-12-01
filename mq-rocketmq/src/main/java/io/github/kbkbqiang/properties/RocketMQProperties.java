package io.github.kbkbqiang.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zhaoqiang
 * @Description
 * @date 2022/11/22 17:47
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "spring.rocketmq")
public class RocketMQProperties {

    private String nameSrvAddr;

    private String accessKey;

    private String secretKey;

    //生产者分组名称
    private String producerGroup;

    //消费者分组名称
    private String consumerGroup;

}
