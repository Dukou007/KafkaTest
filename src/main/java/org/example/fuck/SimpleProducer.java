package org.example.fuck;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        // 配置生产者属性
        Properties props = new Properties();
        // 指定 Kafka 服务器地址
        props.put("bootstrap.servers", "localhost:9092");
        // 消息键序列化器，将字符串序列化为字节
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 消息值序列化器，将字符串序列化为字节
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者实例，指定键和值的类型
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // 发送 10 条消息
            for (int i = 0; i < 10; i++) {
                String key = "key-" + i;
                String value = "Message " + i;
                // 创建消息记录，指定 Topic、键和值
                ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", key, value);

                // 异步发送消息，回调函数处理结果
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        // metadata 包含消息发送的元数据，如 Topic、分区、偏移量
                        System.out.printf("Sent message: key=%s, value=%s, partition=%d, offset=%d%n",
                                key, value, metadata.partition(), metadata.offset());
                    } else {
                        // 打印异常信息
                        exception.printStackTrace();
                    }
                });
            }
            // 确保所有消息发送完成后再关闭生产者
            producer.flush();
        }
    }
}