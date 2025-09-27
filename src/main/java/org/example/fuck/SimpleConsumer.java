package org.example.fuck;

import org.apache.kafka.clients.consumer.*;
import java.util.Arrays;
import java.util.Properties;
import java.time.Duration;

public class SimpleConsumer {
    public static void main(String[] args) {
        // 配置消费者属性
        Properties props = new Properties();
        // 指定 Kafka 服务器地址
        props.put("bootstrap.servers", "localhost:9092");
        // 消费者组 ID，同一组内的消费者分担消息
        props.put("group.id", "test-group");
        // 自动提交偏移量，记录已消费的消息位置
        props.put("enable.auto.commit", "true");
        // 自动提交偏移量的间隔（毫秒）
        props.put("auto.commit.interval.ms", "1000");
        // 键反序列化器，将字节反序列化为字符串
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 值反序列化器，将字节反序列化为字符串
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建消费者实例，指定键和值的类型
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 订阅 Topic，支持多个 Topic
            consumer.subscribe(Arrays.asList("test-topic"));

            // 无限轮询获取消息
            while (true) {
                // 拉取消息，设置超时时间为 100ms
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                // 遍历拉取的消息
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received message: key=%s, value=%s, partition=%d, offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        }
    }
}