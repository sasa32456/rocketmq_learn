package com.n33.rocketmq.spring.provider;

import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
* 未知接口CommandLineRunner
* springboot启动时执行任务CommandLineRunner
 *  如果有多个类实现CommandLineRunner接口，如何保证顺序
 *  需要在实体类上使用一个@Order注解（或者实现Order接口）来表明顺序
 *  已学会（滑稽）
* @author N33
* @date 2019/4/19
*/
@Component
@Order(value=2)
public class UnknowTest implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {

    }



}
