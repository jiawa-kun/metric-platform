package com.jiawa.metric.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 指标平台查询服务的启动类。
 *
 * <p>M0 只搭骨架，确认 Spring Boot 3 能跑起来。
 * D4 会在这里做语义层：根据指标和维度的定义自动生成 SQL。
 */
@SpringBootApplication
public class MetricServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(MetricServiceApplication.class, args);
    }
}
