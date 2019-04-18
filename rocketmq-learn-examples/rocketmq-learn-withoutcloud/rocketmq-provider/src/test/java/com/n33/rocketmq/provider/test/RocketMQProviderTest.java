package com.n33.rocketmq.provider.test;


import com.n33.rocketmq.provider.RocketMQProviderApplication;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.UnsupportedEncodingException;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = RocketMQProviderApplication.class)
//@TestPropertySource("classpath:application.yml")
//@PropertySource("classpath:application.yml")
public class RocketMQProviderTest {

    @Value("${rocketmq.config.nameserAddr}")
    private String nameserAddr;

    @Test
    public void send() throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建DefaultMQProvider
        DefaultMQProducer producer = new DefaultMQProducer("without_cloud_provider");
        //2.设置Namesrv
        producer.setNamesrvAddr(nameserAddr);
        //3.开启DefaultMQProvider
        producer.start();
        //4.创建消息Message
        Message message = new Message("Test_MQ_WithOut_Provider", "Tags", "Key_1", "Hello MQ".getBytes(RemotingHelper.DEFAULT_CHARSET));
        //5.发送消息
        SendResult result = producer.send(message);
        System.out.println(result);
        //6.关闭DefaultMQProvider
        producer.shutdown();

    }
}
