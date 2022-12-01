package io.github.kbkbqiang.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.kbkbqiang.core.DefaultRocketMQListenerContainer;
import io.github.kbkbqiang.core.RocketMQListener;
import io.github.kbkbqiang.core.RocketMQListenerContainer;
import io.github.kbkbqiang.annotion.RocketMQMessageListener;
import io.github.kbkbqiang.properties.RocketMQProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.remoting.RPCHook;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.StandardEnvironment;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhaoqiang
 * @Description
 * @date 2022/11/22 17:43
 */
@Configuration
@EnableConfigurationProperties({RocketMQProperties.class})
@Slf4j
public class RocketMQConfig {

    @Resource
    private RocketMQProperties rocketMQProperties;

    public RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials(rocketMQProperties.getAccessKey(), rocketMQProperties.getSecretKey()));
    }

    @Bean
    @ConditionalOnBean(DefaultMQProducer.class)
    public DefaultMQProducer defaultMQProducer() {
        DefaultMQProducer producer = new DefaultMQProducer(rocketMQProperties.getProducerGroup(), getAclRPCHook(), true, null);
        producer.setNamesrvAddr(rocketMQProperties.getNameSrvAddr());
        producer.setVipChannelEnabled(false);
        try {
            producer.start();
        } catch (Exception e) {
            log.error("rocketmq producer start error", e);
        }
        return producer;
    }

    @PreDestroy
    public void destory() {
        defaultMQProducer().shutdown();
    }

    @Configuration
    public static class ListenerContainerConfiguration implements ApplicationContextAware, InitializingBean {

        private ConfigurableApplicationContext applicationContext;

        private AtomicLong counter = new AtomicLong(0);

        @Resource
        private StandardEnvironment environment;

        @Resource
        private RocketMQProperties rocketMQProperties;

        private ObjectMapper objectMapper;

        @Autowired(required = false)
        public ListenerContainerConfiguration(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void afterPropertiesSet() throws Exception {
            Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(RocketMQMessageListener.class);

            if (Objects.nonNull(beans)) {
                beans.forEach(this::registerContainer);
            }
        }

        private void registerContainer(String beanName, Object bean) {
            Class<?> clazz = AopUtils.getTargetClass(bean);

            if (!RocketMQMessageListener.class.isAssignableFrom(bean.getClass())) {
                throw new IllegalStateException(clazz + " is not instance of " + RocketMQMessageListener.class.getName());
            }
            RocketMQListener rocketMQListener = (RocketMQListener) bean;
            RocketMQMessageListener annotation = clazz.getAnnotation(RocketMQMessageListener.class);
            BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.rootBeanDefinition(RocketMQListenerContainer.class);
            beanBuilder.addPropertyValue("nameSrvAddr", rocketMQProperties.getNameSrvAddr());
            beanBuilder.addPropertyValue("topic", environment.resolvePlaceholders(annotation.topic()));
            beanBuilder.addPropertyValue("tag", environment.resolvePlaceholders(annotation.tag()));
            beanBuilder.addPropertyValue("accessKey", rocketMQProperties.getAccessKey());
            beanBuilder.addPropertyValue("secretKey", rocketMQProperties.getSecretKey());
            beanBuilder.addPropertyValue("consumerGroup", environment.resolvePlaceholders(annotation.group()));
            beanBuilder.addPropertyValue("rocketMQListener", rocketMQListener);
            if (Objects.nonNull(objectMapper)) {
                beanBuilder.addPropertyValue("objectMapper", objectMapper);
            }
            beanBuilder.setDestroyMethodName("destroy");
            String containerBeanName = String.format("%s_%s", DefaultRocketMQListenerContainer.class.getName(), counter.incrementAndGet());
            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getBeanFactory();
            beanFactory.registerBeanDefinition(containerBeanName, beanBuilder.getBeanDefinition());

            DefaultRocketMQListenerContainer container = beanFactory.getBean(containerBeanName, DefaultRocketMQListenerContainer.class);

            if (!container.isStarted()) {
                try {
                    container.start();
                } catch (Exception e) {
                    log.error("started container failed. {}", container, e);
                    throw new RuntimeException(e);
                }
            }
            log.info("register nsq listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
        }

        @Override
        public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            this.applicationContext = (ConfigurableApplicationContext) applicationContext;
        }
    }


}
