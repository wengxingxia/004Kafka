package com.xander.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Description: kafka消费者
 *
 * @author Xander
 * datetime: 2021-01-07 16:53
 */
public class FirstConsumer {

    public static void main(String[] args) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "docker01:9092");
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
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
                }
                // 异步提交偏移量
                consumer.commitAsync();

                // 异步提交偏移量+回调，发送提交请求然后继续做其他事情，如果提交失败，错误信息和偏移量会被记录下来。
                // consumer.commitAsync(new OffsetCommitCallback() {
                //     @Override
                //     public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                //         if(e != null) {
                //             System.out.println("Commit failed for offsets " + map);
                //             e.printStackTrace();
                //         }
                //     }
                // });
                // try {
                //     // 同步提交偏移量
                //     // 处理完当前批次的消息，在轮询更多的消息之前，调用 commitSync()方法提交当前批次最新的偏移量。
                //     consumer.commitSync();
                // } catch (Exception e) {
                //     // 只要没有发生不可恢复的错误， commitSync()方法会一直尝试直至提交成功。如果提交失败，打印错误日志。
                //     System.out.println("commit failed");
                //     e.printStackTrace();
                // }
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
