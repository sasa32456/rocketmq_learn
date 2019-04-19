package com.n33.rocketmq.provider.transaction;


import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * 顺序消费生产者,消费者和普通消费者一致
 *
 * @author N33
 * @date 2019/4/17
 */
public class RocketMQTransactionProviderTest {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException {
        //1.创建DefaultMQProvider
        //DefaultMQProducer producer = new DefaultMQProducer("without_cloud_provider");
        TransactionMQProducer producer = new TransactionMQProducer("without_cloud_transaction_provider");
        //2.设置Namesrv
        producer.setNamesrvAddr("192.168.10.149:9876");

        //指定消息监听对象，用于执行本地事务和消息回传
        TransactionListener transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);


        //线程池
        ExecutorService executorService = new ThreadPoolExecutor(2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("client-transaction-mas-check-thread");
                    return thread;
                }
        );

        producer.setExecutorService(executorService);

        //3.开启DefaultMQProvider
        producer.start();


        //4.创建消息Message
        Message message = new Message("Test_MQ_WithOut_Transaction_Provider",
                "Tags",
                "Key_T",
                ("Hello MQ Transaction!").getBytes(RemotingHelper.DEFAULT_CHARSET));
        //5.发送事物消息
        TransactionSendResult result = producer.sendMessageInTransaction(message, "hello-transaction");
        System.out.println(result);


        //6.关闭DefaultMQProvider
        producer.shutdown();
    }


}


class TransactionListenerImpl implements TransactionListener {

    //存储对应事物状态信息 key:事物ID，value:当前事物执行的状态
    private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();


    /**
     * 执行本地事务
     *
     * @param message
     * @param o
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        //事物ID
        String transactionId = message.getTransactionId();

        //0:执行中，状态未知，1:本地执行成功，2:本地事物执行失败
        localTrans.put(transactionId, 0);

        //业务执行，处理本地事物，service
        System.out.println("hello!---Demo---Transaction");

        try {
            System.out.println("正在执行本地事物---");
//            Thread.sleep(60_000 + 1_000);
            int x = 1 / 0;
            Thread.sleep(1_000);
            System.out.println("正在执行本地事物---成功");
            localTrans.put(transactionId, 1);
//        } catch (InterruptedException e) {
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("失败" + e);
            localTrans.put(transactionId, 2);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        return LocalTransactionState.ROLLBACK_MESSAGE;
    }

    /**
     * 消息回传
     *
     * @param messageExt
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {

        //获取对应事物ID事物的状态
        final String transactionId = messageExt.getTransactionId();
        //获取对应事物ID的执行状态
        final Integer status = localTrans.get(transactionId);

        System.out.println("消息回查-----transactionId: " + transactionId + " ,状态 " + status);

        switch (status) {
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        return LocalTransactionState.UNKNOW;
    }
}
