package com.alibaba.rocketmq.example.quickstart.test2;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

/**
 * Created by lybuestc on 17/3/16.
 */
public class PullConsumer {

    public static void main(String[] args) {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(
                "PullConsumer");
        consumer.setNamesrvAddr("localhost:9876");

        try {
            consumer.start();

            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TestTopicA");
            for(MessageQueue mq : mqs) {
                System.out.println("Consumer From the queue:" + mq);
                long offset = 0;
                PullResult result = consumer.pullBlockIfNotFound(mq, null, offset, 32);
                List<MessageExt> msgs = result.getMsgFoundList();
                if(msgs!=null && msgs.size() != 0) {
                    for(MessageExt msg : msgs) {
                        System.out.println(new String(msg.getBody(), Charset.forName("utf-8")));
                    }
                }
                offset = result.getNextBeginOffset();
                System.out.println(result.getPullStatus());
            }


        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}