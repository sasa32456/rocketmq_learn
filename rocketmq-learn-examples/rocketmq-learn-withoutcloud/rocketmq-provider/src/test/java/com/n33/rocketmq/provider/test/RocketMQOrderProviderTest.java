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
import org.springframework.test.context.junit4.SpringRunner;

import java.io.UnsupportedEncodingException;

/**
 * 顺序消费生产者
 *
 * @author N33
 * @date 2019/4/17
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = RocketMQProviderApplication.class)
//@TestPropertySource("classpath:application.yml")
//@PropertySource("classpath:application.yml")
public class RocketMQOrderProviderTest {

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


        for (int i = 0; i < 5; i++) {
            //4.创建消息Message
            Message message = new Message("Test_MQ_WithOut_Order_Provider",
                    "Tags",
                    "Key_" + i,
                    ("Hello MQ !" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //5.发送消息
            //第一个参数，发送的消息信息
            //第二个参数，选中指定的消息队列
            //第三个参数，指定队列下标
            SendResult result = producer.send(message, (mqs, msg, arg) -> {
                //获取队列的下标
                Integer index = (Integer) arg;
                //获取对应下标的队列
                return mqs.get(index);
            }, 1);
            System.out.println(result);
        }


        //6.关闭DefaultMQProvider
        producer.shutdown();

    }
}
