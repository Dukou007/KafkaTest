package org.example.fuck;


import org.apache.kafka.*;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaExample {

    // Kafka服务器的地址，使用你的Ubuntu服务器IP
    private static final String BOOTSTRAP_SERVERS = "192.168.18.133:9092";
    // 要发送和接收消息的主题
    private static final String TOPIC_NAME = "test-topic";
    // 消费组ID，同一个组内的消费者会共享消息
    private static final String GROUP_ID = "my-first-consumer-group";

    public static void main(String[] args) {
        System.out.println("---- 启动 Kafka 生产者和消费者 ----");

        // 使用一个线程池，可以同时运行多个任务（这里是生产者和消费者）
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // 提交消费者任务到线程池，使其开始接收消息
        executor.submit(new ConsumerTask());

        // 提交生产者任务到线程池，使其开始发送消息
        executor.submit(new ProducerTask());

        // 让线程池运行一段时间，比如 20 秒
        try {
            executor.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 20秒后关闭线程池
        executor.shutdownNow();
    }

    // 生产者任务类，实现了 Runnable 接口
    static class ProducerTask implements Runnable {
        @Override
        public void run() {
            // 配置生产者属性
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            // key.serializer：指定用于序列化消息键的类
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            // value.serializer：指定用于序列化消息值的类
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            System.out.println(StringSerializer.class.getName() + "----------------------");
            System.out.println(StringSerializer.class.getName() + "----------------------");
            // 创建一个 Kafka 生产者实例
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            try {
                int messageCount = 5;
                for (int i = 0; i < messageCount; i++) {
                    String messageKey = "键-" + i;
                    String messageValue = "你好，Kafka！这是我的第 " + i + " 条消息。";
                    // ProducerRecord 包含主题、消息键和消息值
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue);

                    // 发送消息，并使用 .get() 方法同步等待发送结果，确保消息发送成功
                    producer.send(record).get();
                    System.out.println("消息发送成功：" + messageValue);
                }
            } catch (Exception e) {
                System.err.println("生产者发生错误：" + e.getMessage());
                e.printStackTrace();
            } finally {
                // 确保所有待发送的消息都已发送完成
                producer.flush();
                // 关闭生产者
                producer.close();
            }
        }
    }

    // 消费者任务类，实现了 Runnable 接口
    static class ConsumerTask implements Runnable {
        @Override
        public void run() {
            // 配置消费者属性
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            // key.deserializer：指定用于反序列化消息键的类
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            // value.deserializer：指定用于反序列化消息值的类
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            // auto.offset.reset：当没有找到之前的消费偏移量时，从主题的开头开始消费
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // 创建一个 Kafka 消费者实例
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            // 订阅主题
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            System.out.println("消费者已订阅主题：" + TOPIC_NAME);

            try {
                // 持续轮询，直到线程被中断
                while (!Thread.currentThread().isInterrupted()) {
                    // poll() 方法会等待一段时间，直到拉取到消息或超时
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    if (!records.isEmpty()) {
                        System.out.println("--- 收到新消息 ---");
                        // 遍历并处理每条消息
                        records.forEach(record -> {
                            System.out.printf("  分区：%d, 偏移量：%d, 键：%s, 值：%s%n",
                                    record.partition(),
                                    record.offset(),
                                    record.key(),
                                    record.value());
                        });
                    }
                }
            } finally {
                // 关闭消费者
                consumer.close();
                System.out.println("消费者已关闭。");
            }
        }
    }
   }