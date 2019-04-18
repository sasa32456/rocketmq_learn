package com.n33.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
* 无法测试
*
* @author N33
* @date 2019/4/18
*/
@RunWith(SpringRunner.class)
public class RocketMQConsumerTest {

    @Value("${rocketmq.config.nameserAddr}")
    private String nameserAddr;

    @Test
    public void listen() throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建DefaultMQPushConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("without_cloud_consumer");
        //2.设置Namesrv
        consumer.setNamesrvAddr(nameserAddr);

        //设置消息拉取上限
        consumer.setConsumeMessageBatchMaxSize(2);

        //3.设置subscribe,这里是要读取的主题信息
        consumer.subscribe("Test_MQ_WithOut_Provider",//消费的主题
                "Tags || *");//过滤的标签
        //4.创建消息监听MessageListener
        consumer.setMessageListener(new MessageListenerConcurrently() {
            //5.获取消息
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //迭代消息
                for (MessageExt msg : msgs) {
                    try {
                        //获取重试次数
                        int reconsumeTimes = msg.getReconsumeTimes();
                        //获取主题
                        String topic = msg.getTopic();
                        //获取标签
                        String tags = msg.getTags();
                        //获取信息
                        byte[] body = msg.getBody();
                        String result = new String(body, RemotingHelper.DEFAULT_CHARSET);

                        System.out.println("Consumer消费信息: topic" + topic + " , tags: " + tags + " ,result: " + result + " , reconsumeTimes: " + reconsumeTimes);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        //消息重试
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                //6.返回消息读取状态
                //消费完成
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //开启Consumer
        consumer.start();

    }
}
