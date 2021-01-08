package com.xander.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Description: kafka消费者: 提交特定的偏移量
 *
 * @author Xander
 * datetime: 2021-01-07 16:53
 */
public class CommitSpecificOffset {

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private static int count = 0;

    public static void main(String[] args) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "docker01:9092");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group2");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//从头开始读取kafka消息
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.subscribe(Collections.singletonList("test"));


        try {
            while (true) {
                //200ms轮询一次
                ConsumerRecords<String, String> rec = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> r : rec) {
                    System.out.println("\n--------------------------------------------");
                    System.out.println("主题：" + r.topic());
                    System.out.println("分区：" + r.partition());
                    System.out.println("偏移量：" + r.offset());
                    System.out.println("key：" + r.key());
                    System.out.println("value：" + r.value());
                    System.out.println(r.value());
                    // 记录每个分区的偏移量
                    currentOffsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1, "no metadata"));
                    if (count % 1000 == 0) {
                        //每处理1000条记录，提交一次偏移量
                        consumer.commitAsync(currentOffsets, null);
                    }
                    count++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
