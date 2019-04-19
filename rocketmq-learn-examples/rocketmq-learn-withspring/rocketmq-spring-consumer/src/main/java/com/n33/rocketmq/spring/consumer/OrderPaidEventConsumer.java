package com.n33.rocketmq.spring.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

@Service
@RocketMQMessageListener(topic = "${demo.rocketmq.orderTopic}", consumerGroup = "order-paid-consumer")
public class OrderPaidEventConsumer implements RocketMQListener<String> {
    @Override
    public void onMessage(String s) {
        System.out.printf("------- OrderPaidEventConsumer received: %s \n", s);
    }
}
