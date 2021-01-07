package com.xander.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Description: 发送并忘记
 *
 * @author Xander
 * datetime: 2021-01-07 10:01
 */
public class SendAndForget {

    public static void main(String[] args) throws InterruptedException {

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
        ProducerRecord<String, String> rec = new ProducerRecord<>("test", "SendAndForgetKey", "SendAndForgetMsg1");

        // 发送并忘记
        // send() 方法会返回一个包含 RecordMetadata 的 Future 对象，
        // 不过因为我们会忽略返回值，所以无法知道消息是否发送成功。如果不关心发送结果，那么可以使用这种发送方式
        pd.send(rec);

        Thread.sleep(20);
    }
}
