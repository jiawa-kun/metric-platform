package com.jiawa.metric.generator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 对 {@link KafkaProducer} 的一层薄封装，核心配置都写在 {@link #buildProperties(Config)} 里。
 *
 * <p>发送是异步的：{@link KafkaProducer#send} 只是把消息放进客户端缓冲区，真正的网络写出在后台线程完成。
 * 所以统计成功/失败要在 callback 里做，结束前必须 {@link #flush()}，否则进程退出时缓冲区里可能还有没发出去的消息。
 */
public final class OrderProducer implements AutoCloseable {

    private final Config config;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AtomicLong success = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    /** 每个分区成功写入了多少条。key 是分区号。callback 在 I/O 线程执行，所以用并发 Map。 */
    private final ConcurrentHashMap<Integer, AtomicLong> perPartition = new ConcurrentHashMap<>();

    public OrderProducer(Config config) {
        this.config = config;
        this.producer = new KafkaProducer<>(buildProperties(config));
    }

    /**
     * 这里的每一项都建议在面试里能讲清楚「是什么、为什么这么设」。
     */
    static Properties buildProperties(Config config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks=all：ISR（同步副本集合）里的所有副本都写下这条消息，才通知生产者成功。
        // 可靠性最高，延迟也相对最高。单机只有一个副本，效果和 acks=1 几乎一样，但配置要按「以后会有副本」来写。
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 幂等生产者：broker 会记住这个生产者的 PID + 序号，网络重试时不会写出重复消息。
        // 打开后 Kafka 会强制 acks=all，retries > 0，且 max.in.flight.requests.per.connection <= 5。
        // 注意：这只保证「同一个生产者进程生命周期内」不重复；进程重启后 PID 会变，跨会话去重要靠事务或下游去重。
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 消息不是一条一条立刻发出，而是按分区攒成 batch。
        // linger.ms：最多再等这么久，让更多消息凑进同一批。QPS 低时会略增延迟，QPS 高时明显提升吞吐。
        // batch.size：单个分区一批的上限（字节）。到了这个大小就立刻发送，不必再等 linger。
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);

        // 客户端缓冲区上限。如果发送速度快于网络，这里会先堆着；堆满后 send() 会阻塞。
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "order-generator");
        return props;
    }

    /**
     * 异步发送一条订单。
     *
     * <p>key 用 shopId，不顶 orderId：Kafka 对 key 做哈希来选分区，同一个店铺的订单会落到同一分区，
     * 分区内有序。后面按店铺做聚合（Flink / 离线对账）时，不必跨分区。
     * 代价是店铺之间如果流量差很大，分区可能不够均匀。若改成 orderId，负载更均匀，但同一店铺会打散。
     */
    public void send(OrderEvent event) {
        String json = toJson(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(config.topic, event.shopId, json);
        producer.send(record, new CountingCallback());
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        // close 会先 flush 再关闭后台线程，超时则强制退出
        producer.close(Duration.ofSeconds(15));
    }

    public long successCount() {
        return success.get();
    }

    public long failedCount() {
        return failed.get();
    }

    public void printPartitionSummary() {
        System.out.println("各分区成功写入条数：");
        perPartition.entrySet().stream()
                .sorted((a, b) -> Integer.compare(a.getKey(), b.getKey()))
                .forEach(e -> System.out.println("  partition " + e.getKey() + " = " + e.getValue().get()));
    }

    private String toJson(OrderEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("订单序列化失败: " + event.orderId, e);
        }
    }

    private final class CountingCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                failed.incrementAndGet();
                System.err.println("发送失败: " + exception.getMessage());
                return;
            }
            success.incrementAndGet();
            perPartition.computeIfAbsent(metadata.partition(), k -> new AtomicLong()).incrementAndGet();
        }
    }
}
