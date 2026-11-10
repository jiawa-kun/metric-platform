package com.jiawa.metric.generator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 生成模拟订单。店铺、商品、渠道都是写死的小集合，方便后面按维度聚合时肉眼能对得上。
 *
 * <p>orderId 带时间戳和序号，便于排查；金额 = 数量 × 单价，保留两位小数。
 */
public final class OrderEventFactory {

    private static final List<String> SHOPS = List.of(
            "S001", "S002", "S003", "S004", "S005",
            "S006", "S007", "S008", "S009", "S010",
            "S011", "S012", "S013", "S014", "S015",
            "S016", "S017", "S018", "S019", "S020"
    );

    private static final List<String> CHANNELS = List.of("APP", "H5", "MINI_PROGRAM", "OFFLINE");

    /** 状态带权重：大部分已支付，少量待支付或取消，后面做 GMV 口径时可以排除非 PAID。 */
    private static final String[] STATUSES = {"PAID", "PAID", "PAID", "PAID", "PAID", "PAID", "PAID", "PAID", "CREATED", "CANCELLED"};

    private final AtomicLong sequence = new AtomicLong(1);

    public OrderEvent next() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        String shopId = pick(SHOPS, random);
        // SKU0001 .. SKU0100，单价随编号变化，便于演示「按商品聚合」
        int skuNo = random.nextInt(1, 101);
        String skuId = String.format("SKU%04d", skuNo);
        String channel = pick(CHANNELS, random);
        String userId = "U" + random.nextInt(1, 5001);
        int quantity = random.nextInt(1, 6);
        BigDecimal unitPrice = BigDecimal.valueOf(9.9 + (skuNo % 20) * 5L)
                .setScale(2, RoundingMode.HALF_UP);
        BigDecimal amount = unitPrice.multiply(BigDecimal.valueOf(quantity))
                .setScale(2, RoundingMode.HALF_UP);
        String status = STATUSES[random.nextInt(STATUSES.length)];

        // event time 略早于当前时间（0~3 秒），模拟「业务发生」和「写入 Kafka」之间的缝隙。
        // 截断到毫秒，写成 UTC 的 ISO-8601（例如 2026-11-11T00:19:54.660Z）。
        // D3 学 Flink 水位线时，这个字段就是 event time。
        String eventTime = Instant.now()
                .minus(random.nextInt(0, 3000), ChronoUnit.MILLIS)
                .truncatedTo(ChronoUnit.MILLIS)
                .toString();

        long seq = sequence.getAndIncrement();
        // 例：O-20261111T001954-1，便于肉眼排查，不是业务主键规则。
        String stamp = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());
        String orderId = "O-" + stamp + "-" + seq;

        return new OrderEvent(orderId, shopId, skuId, channel, userId, quantity, amount, status, eventTime);
    }

    private static String pick(List<String> list, ThreadLocalRandom random) {
        return list.get(random.nextInt(list.size()));
    }
}
