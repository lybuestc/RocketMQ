package com.alibaba.rocketmq.example.quickstart.test2;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.Charset;

/**
 * Created by lybuestc on 17/3/16.
 */
public class Producer {

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("Producer");
        producer.setNamesrvAddr("localhost:9876");

        try {

            producer.start();

            for(int i=0; i<20; i++) {

                Message msg = new Message("TestTopicA", "Push", "test1",
                        "Test Msg 1".getBytes(Charset.forName("utf-8")));

                SendResult result = producer.send(msg);
                System.out.println("id:" + result.getMsgId() +
                        " result:" + result.getSendStatus());

                msg = new Message("TestTopicA", "Pull", "test2",
                        "Test Msg 2".getBytes(Charset.forName("utf-8")));



                result = producer.send(msg);

                System.out.println("id:" + result.getMsgId() +
                        " result:" + result.getSendStatus());
                Thread.sleep(3000);
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.shutdown();
        }
        try {
            Thread.sleep(2000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}