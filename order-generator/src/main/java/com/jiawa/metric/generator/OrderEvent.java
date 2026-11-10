package com.jiawa.metric.generator;

import java.math.BigDecimal;

/**
 * 一条模拟订单事件，对应 Kafka 消息的 value（JSON）。
 *
 * <p>字段是后面指标平台会用到的最小集合：谁（店铺/用户/渠道）在什么时候买了什么、多少钱、什么状态。
 * 金额用 {@link BigDecimal}，不要用 double，避免 0.1 + 0.2 这类精度问题传到下游。
 *
 * <p>这是脚本生成的模拟数据，不是真实订单。JSON 里不加多余字段，简历和面试里也要如实说明。
 */
public final class OrderEvent {

    public final String orderId;
    public final String shopId;
    public final String skuId;
    public final String channel;
    public final String userId;
    public final int quantity;
    public final BigDecimal amount;
    public final String status;
    /** UTC 时间，ISO-8601，例如 2026-11-11T00:20:00Z。这是事件发生时间（event time），不是写入 Kafka 的时间。 */
    public final String eventTime;

    public OrderEvent(String orderId, String shopId, String skuId, String channel, String userId,
                      int quantity, BigDecimal amount, String status, String eventTime) {
        this.orderId = orderId;
        this.shopId = shopId;
        this.skuId = skuId;
        this.channel = channel;
        this.userId = userId;
        this.quantity = quantity;
        this.amount = amount;
        this.status = status;
        this.eventTime = eventTime;
    }
}
