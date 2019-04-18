package com.n33.rocketmq.provider.broadcasting;


import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * 广播模式
 *
 * @author N33
 * @date 2019/4/18
 */
public class RocketMQBroadcastingProviderTest {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException, UnsupportedEncodingException {

        //1.创建DefaultMQProvider
        DefaultMQProducer producer = new DefaultMQProducer("without_cloud_broadcasting_provider");
        //2.设置Namesrv
        producer.setNamesrvAddr("192.168.10.149:9876");
        //3.开启DefaultMQProvider
        producer.start();
        //4.创建消息Message
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("Test_MQ_WithOut_broadcasting_Provider",
                    "Tags",
                    "Key_" + i,
                    ("Hello MQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            messages.add(message);
        }
        //5.批量发送消息
        SendResult result = producer.send(messages);
        System.out.println(result);

        //6.关闭DefaultMQProvider
        producer.shutdown();
    }
}
