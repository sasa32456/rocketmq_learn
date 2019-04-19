package com.n33.rocketmq.spring.provider;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@RestController
@EnableAsync
public class RocketMQSpringProviderApplication {
    public static void main(String[] args) {
        SpringApplication.run(RocketMQSpringProviderApplication.class, args);
    }


    private static final String TX_PGROUP_NAME = "myTxProducerGroup";

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Value("${rocketmq.transTopic}")
    private String springTransTopic;
    @Value("${rocketmq.topic}")
    private String springTopic;
    @Value("${rocketmq.orderTopic}")
    private String orderPaidTopic;
    @Value("${rocketmq.msgExtTopic}")
    private String msgExtTopic;


    @GetMapping(value = "sendUserDefined/{msg}")
    public String sendUserDefined(@PathVariable String msg) {
        System.out.println("333");
        rocketMQTemplate.asyncSend(orderPaidTopic, "1", new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.printf("async onSucess SendResult=%s %n", sendResult);
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.printf("async onException Throwable=%s %n", throwable);
            }
        });
        System.out.println("666");
        return "Success";
    }

    @GetMapping(value = "sendTransactional/{msg1}")
    public String sendTransactional(@PathVariable String msg1) {
        testTransaction(msg1);
        return "Success";
    }


    private void testTransaction(String msg1) {
        System.out.println(msg1 + "=========================================" + springTransTopic);

                Message msg = MessageBuilder.withPayload("Hello RocketMQ ").
                        setHeader(RocketMQHeaders.KEYS, "KEY_t").build();
                SendResult sendResult = rocketMQTemplate.sendMessageInTransaction(TX_PGROUP_NAME,
                        springTransTopic, msg, "hello-transaction");
                System.out.printf("------ send Transactional msg body = %s , sendResult=%s %n",
                        msg.getPayload(), sendResult.getSendStatus());

    }

    @RocketMQTransactionListener(txProducerGroup = TX_PGROUP_NAME)
    class TransactionListenerImpl implements RocketMQLocalTransactionListener {
        private AtomicInteger transactionIndex = new AtomicInteger(0);

        private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<String, Integer>();

        @Override
        public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {

            System.out.println("执行本地业务");
            //假装异常
            int i = 1/0;
            System.out.println("执行本地业务结束");

            //String transId = (String) msg.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);//我日，居然还有这种错误
            String transId = (String) msg.getHeaders().get("rocketmq_TRANSACTION_ID");
            System.out.printf("#### executeLocalTransaction is executed, msgTransactionId=%s %n",
                    transId);
            System.out.println( "1--------------------------------------");
            int value = transactionIndex.getAndIncrement();
            System.out.println( "2--------------------------------------");
            int status = value % 3;
            localTrans.put(transId, status);
            System.out.println(status + "--------------------------------------");
            if (status == 0) {
                // Return local transaction with success(commit), in this case,
                // this message will not be checked in checkLocalTransaction()
                System.out.printf("    # COMMIT # Simulating msg %s related local transaction exec succeeded! ### %n", msg.getPayload());
                return RocketMQLocalTransactionState.COMMIT;
            }

            if (status == 1) {
                // Return local transaction with failure(rollback) , in this case,
                // this message will not be checked in checkLocalTransaction()
                System.out.printf("    # ROLLBACK # Simulating %s related local transaction exec failed! %n", msg.getPayload());
                return RocketMQLocalTransactionState.ROLLBACK;
            }


            System.out.printf("    # UNKNOW # Simulating %s related local transaction exec UNKNOWN! \n");
            return RocketMQLocalTransactionState.UNKNOWN;
        }

        @Override
        public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
            String transId = (String) msg.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);
            RocketMQLocalTransactionState retState = RocketMQLocalTransactionState.COMMIT;
            Integer status = localTrans.get(transId);
            if (null != status) {
                switch (status) {
                    case 0:
                        retState = RocketMQLocalTransactionState.UNKNOWN;
                        break;
                    case 1:
                        retState = RocketMQLocalTransactionState.COMMIT;
                        break;
                    case 2:
                        retState = RocketMQLocalTransactionState.ROLLBACK;
                        break;
                }
            }
            System.out.printf("------ !!! checkLocalTransaction is executed once," +
                            " msgTransactionId=%s, TransactionState=%s status=%s %n",
                    transId, retState, status);
            return retState;
        }
    }

}
