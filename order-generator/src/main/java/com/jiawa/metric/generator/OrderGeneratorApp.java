package com.jiawa.metric.generator;

/**
 * 订单生成器入口：建 topic（如有必要）→ 按指定 QPS 往 Kafka 写模拟订单 → 打印实测吞吐。
 *
 * <p>限速方法：用「下一条应该发送的时间点」来 sleep，而不是每次固定 sleep(1000/qps)。
 * 后者会把 sleep 的误差累积进去，跑久了实际 QPS 会偏低。
 */
public final class OrderGeneratorApp {

    public static void main(String[] args) {
        Config config = Config.fromArgs(args);
        config.logOnStart("order-generator");

        KafkaTopicAdmin.ensureTopic(config);

        OrderEventFactory factory = new OrderEventFactory();
        long startNanos = System.nanoTime();
        long sent = 0;

        try (OrderProducer producer = new OrderProducer(config)) {
            // 两条消息之间的目标间隔（纳秒）
            long intervalNanos = 1_000_000_000L / config.qps;
            long nextDeadline = System.nanoTime();

            while (config.count == 0 || sent < config.count) {
                waitUntil(nextDeadline);
                producer.send(factory.next());
                sent++;
                nextDeadline += intervalNanos;

                if (sent % Math.max(config.qps, 1) == 0) {
                    double elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;
                    double rate = elapsedSec > 0 ? sent / elapsedSec : 0;
                    System.out.printf("已提交 %d 条，耗时 %.2f s，提交速率 %.1f 条/秒%n", sent, elapsedSec, rate);
                }
            }

            // 等待客户端把缓冲区里的消息全部发给 broker，callback 都会跑完
            producer.flush();

            double elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;
            double rate = elapsedSec > 0 ? producer.successCount() / elapsedSec : 0;
            System.out.println();
            System.out.println("=== 发送完成 ===");
            System.out.printf("目标 QPS        = %d%n", config.qps);
            System.out.printf("已调用 send     = %d%n", sent);
            System.out.printf("broker 确认成功 = %d%n", producer.successCount());
            System.out.printf("失败            = %d%n", producer.failedCount());
            System.out.printf("墙钟耗时        = %.3f s%n", elapsedSec);
            System.out.printf("实测吞吐        = %.1f 条/秒（含 flush 等待）%n", rate);
            producer.printPartitionSummary();
        }
    }

    /** 如果还没到 deadline，就睡到那个时间点；已经超时则立刻返回（避免欠账越积越多）。 */
    private static void waitUntil(long deadlineNanos) {
        while (true) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            try {
                long millis = remaining / 1_000_000L;
                int nanos = (int) (remaining % 1_000_000L);
                Thread.sleep(millis, nanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("限速等待被中断", e);
            }
        }
    }
}
