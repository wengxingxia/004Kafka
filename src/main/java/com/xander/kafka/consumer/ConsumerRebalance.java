package com.xander.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Description: 再均衡监昕器
 *
 * @author Xander
 * datetime: 2021-01-07 19:59
 */
public class ConsumerRebalance {

    public static void main(String[] args) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "docker01:9092");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group2");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//从头开始读取kafka消息
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);

        // 已经处理的消息偏移量
        final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        // 再均衡监昕器
        ConsumerRebalanceListener handleRebalance = new ConsumerRebalanceListener() {

            /**
             * 在再均衡开始之前和消费者停止读取消息之后被调用。如果在这里提交偏移量，下一个接
             * 管分区的消费者就知道该从哪里开始读取了。
             *
             * 如果发生再均衡，我们要在即将失去分区所有权时提交偏移量。要注意，提交的是最近
             * 处理过的偏移量，而不是批次中还在处理的最后一个偏移量。因为分区有可能在我们还
             * 在处理消息的时候被撤回。我们要提交所有分区的偏移量 ，而不只是那些即将失去所有
             * 权的分区的偏移量一一因为提交的偏移量是已经处理过的，所以不会有什么问题。调用
             * commitSync()方法，确保在再均衡发生之前提交偏移量。
             * @param collection
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("ConsumerRebalanceListener---onPartitionsRevoked");
                System.out.println("Lost partitions in rebalance. Committing current offsets:" + currentOffsets);
                // 提交已经处理的消息偏移量
                consumer.commitSync(currentOffsets);
            }


            /**
             * 在重新分配分区之后和消费者开始读取消息之前被调用。
             * @param collection
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("ConsumerRebalanceListener---onPartitionsAssigned");
            }
        };

        List<String> topics = Collections.singletonList("test");
        // 订阅的主题列表+监听在均衡监听器
        consumer.subscribe(topics, handleRebalance);

        try {
            while (true) {
                ConsumerRecords<String, String> rec = consumer.poll(100);
//                System.out.println("We got record count " + rec.count());
                for (ConsumerRecord<String, String> r : rec) {
                    System.out.println("\n--------------------------------------------");
                    System.out.println("主题：" + r.topic());
                    System.out.println("分区：" + r.partition());
                    System.out.println("偏移量：" + r.offset());
                    System.out.println("key：" + r.key());
                    System.out.println("value：" + r.value());
                    System.out.println(r.value());
                    TimeUnit.MILLISECONDS.sleep(5000);
                    currentOffsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1, "no metadata"));
                }
                // 异步提交偏移量
                consumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // 同步提交偏移量
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }
}

