package com.jiawa.metric.generator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * M0 验证用的消费者：订阅 ods_order，按分区统计收到的条数，然后退出。
 *
 * <p>几个面试常问的概念：
 * <ul>
 *   <li>consumer group：同一个 group.id 的消费者共同消费一个 topic，一条消息只会被组内一个消费者拿到。
 *       组用「每个分区的位移（offset）」记住读到了哪里。</li>
 *   <li>如果沿用旧的 group.id，且上次已经把位移提交到末尾，这次再跑就看不到历史消息。</li>
 *   <li>所以本程序默认每次生成一个新的 group.id；加上 --from-beginning 后，会在分配到分区时从最早的消息读起。</li>
 * </ul>
 *
 * <p>退出条件：收到 --expect 条，或连续 --idle-sec 秒没有新消息。
 */
public final class SimpleConsumer {

    public static void main(String[] args) {
        Config config = Config.fromArgs(args);
        String groupId = (config.groupId == null || config.groupId.isBlank())
                ? "m0-verifier-" + UUID.randomUUID()
                : config.groupId;

        System.out.println("=== SimpleConsumer ===");
        System.out.println("bootstrapServers = " + config.bootstrapServers);
        System.out.println("topic            = " + config.topic);
        System.out.println("groupId          = " + groupId);
        System.out.println("fromBeginning    = " + config.fromBeginning);
        System.out.println("expect           = " + (config.expect == 0 ? "不限制，靠空闲退出" : config.expect));
        System.out.println("idleSec          = " + config.idleSec);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // 只对「从未提交过位移的分区」生效。新 group 会从 latest 开始，所以验证历史数据必须自己 seek。
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.fromBeginning ? "earliest" : "latest");
        // 验证程序读完就退出，不必定期提交位移；关掉自动提交，概念更干净。
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "m0-verifier");

        Map<Integer, Long> perPartition = new TreeMap<>();
        long total = 0;
        long startMillis = System.currentTimeMillis();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(java.util.List.of(config.topic));

            if (config.fromBeginning) {
                // subscribe 是异步的，必须先 poll 一次等到分区分配完成，再 seek 才有效。
                waitForAssignment(consumer);
                Set<TopicPartition> assigned = consumer.assignment();
                consumer.seekToBeginning(assigned);
                System.out.println("已分配分区 " + assigned.stream().map(TopicPartition::partition).sorted().toList()
                        + "，已 seek 到 beginning");
            }

            Instant lastMessageAt = Instant.now();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    if (Duration.between(lastMessageAt, Instant.now()).toSeconds() >= config.idleSec) {
                        System.out.println("连续 " + config.idleSec + " 秒没有新消息，退出。");
                        break;
                    }
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    perPartition.merge(record.partition(), 1L, Long::sum);
                    total++;
                    lastMessageAt = Instant.now();
                }
                System.out.println("累计收到 " + total + " 条");

                if (config.expect > 0 && total >= config.expect) {
                    System.out.println("已达到期望条数 " + config.expect + "，退出。");
                    break;
                }
            }
        }

        double elapsedSec = (System.currentTimeMillis() - startMillis) / 1000.0;
        System.out.println();
        System.out.println("=== 消费完成 ===");
        System.out.printf("合计            = %d 条%n", total);
        System.out.printf("墙钟耗时        = %.3f s%n", elapsedSec);
        System.out.println("各分区收到条数：");
        if (perPartition.isEmpty()) {
            System.out.println("  （没有收到任何消息。若刚发送过，请加上 --from-beginning）");
        } else {
            perPartition.forEach((p, n) -> System.out.println("  partition " + p + " = " + n));
            System.out.println("覆盖分区: " + perPartition.keySet().stream().map(String::valueOf).collect(Collectors.joining(", ")));
        }
    }

    /** 最多等 10 秒，直到消费者拿到分区分配。 */
    private static void waitForAssignment(KafkaConsumer<String, String> consumer) {
        Instant deadline = Instant.now().plusSeconds(10);
        while (consumer.assignment().isEmpty()) {
            if (Instant.now().isAfter(deadline)) {
                throw new IllegalStateException("等待分区分配超时，请确认 topic 已存在且 broker 可连");
            }
            consumer.poll(Duration.ofMillis(200));
        }
    }
}
