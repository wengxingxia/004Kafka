package com.xander.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Description: 异步发送
 *
 * @author Xander
 * datetime: 2021-01-07 10:01
 */
public class AsyncSend {

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
        ProducerRecord<String, String> rec = new ProducerRecord<>("test", "asyncKey", "asyncMsg1");

        // 异步发送，并设置回调
        pd.send(rec, new MyCallback());
        pd.close();

    }
}

// 异步发送回调
class MyCallback implements Callback {

    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if (exception != null)//发送失败
            exception.printStackTrace();
        else {//发生成功
            System.out.println("Message posted call back success");
            System.out.println(recordMetadata.topic() + "--" + recordMetadata.partition() + "---" + recordMetadata.offset());
        }
    }
}
