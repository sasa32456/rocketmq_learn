package com.n33.rocketmq.consumer.broadcasting;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class RocketMQBroadcastingConsumerServiceTestB {

    public static void main(String[] args) throws MQClientException {
        //1.创建DefaultMQPushConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("without_cloud_broadcasting_provider");
        //2.设置Namesrv
        consumer.setNamesrvAddr("192.168.10.149:9876");

        //默认是集群消费模式,改成广播模式
        consumer.setMessageModel(MessageModel.BROADCASTING);

        //设置消息拉取上限
        consumer.setConsumeMessageBatchMaxSize(2);

        //3.设置subscribe,这里是要读取的主题信息
        consumer.subscribe("Test_MQ_WithOut_broadcasting_Provider",//消费的主题
                "*");//过滤的标签 "xx || xx"
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

                        System.out.println("B-------Consumer消费信息: topic" + topic + " , tags: " + tags + " ,result: " + result + " , reconsumeTimes: " + reconsumeTimes);

                        //假装异常
                        //int x = 1/0;
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        //输出异常
                        System.out.println(e);
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
