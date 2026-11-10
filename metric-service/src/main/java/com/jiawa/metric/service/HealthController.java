package com.jiawa.metric.service;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 最简单的探活接口，用来确认服务已启动。
 *
 * <p>没有用 Actuator，是为了少一层「框架魔法」：就是一个普通的 Controller。
 * 访问 http://localhost:8080/health 应返回 {"status":"UP"}。
 */
@RestController
public class HealthController {

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP");
    }
}
