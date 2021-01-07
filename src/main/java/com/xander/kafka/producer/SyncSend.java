package com.xander.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Description: 同步发送
 *
 * @author Xander
 * datetime: 2021-01-07 10:01
 */
public class SyncSend {

    public static void main(String[] args) {

        Properties p = new Properties();
        // 定义 Kafka 生产者有 3个必选的属性
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "docker01:9092");// broker 的地址清单
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //创建生产者
        Producer<String, String> pd = new KafkaProducer<>(p);
        //定义一个消息，指定 topic、key、value
        // key可以设置，也可以不设置
        // 如果主题不存在，默认情况下 server.properties 中 auto.create.topics.enable为true，会自动创建主题
        ProducerRecord<String, String> rec = new ProducerRecord<>("test", "syncKey", "syncMsg1");

        // 同步发送，并等待响应
        try {
            // RecordMetaData包含了主题和分区信息，以及记录在分区里的偏移量
            RecordMetadata recordMetadata = pd.send(rec).get();
            System.out.println(recordMetadata.topic() + "--" + recordMetadata.partition() + "---" + recordMetadata.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }
}
