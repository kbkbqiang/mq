package io.github.kbkbqiang.annotion;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author zhaoqiang
 * @Description
 * @date 2022/11/22 18:27
 */

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RocketMQMessageListener {

    /**
     * Topic name
     *
     * @return
     */
    String topic();

    /**
     * *代表全部的Tag
     *
     * @return
     */
    String tag();

    /**
     * 分组
     *
     * @return
     */
    String group();
}
