# metric-platform

把随州「五级元数据驱动指标口径」的真实经验，延伸成一个带语义层的指标平台。目标岗位：兴趣岛系统开发工程师（数据产品后台）。

业务数据用电商订单（贴近 OMS 经历），**由 `order-generator` 脚本模拟生成，不是真实订单**。简历和面试里如实说明。

本仓库按一周面试节奏压缩为 5 天。当前完成的是 **D1 / M0**：本地基础设施 + 订单生成器写入 Kafka。

## 整体架构（5 天精简版）

```
订单模拟生成器                实时（D3 最小作业）
(order-generator)  ──► Kafka ──► Flink ──► ClickHouse（ADS 查询）
                         │
                         └── 离线链路（D3 只学原理，不实现）
                             Spark / Hive：ODS → DWD → DWS → ADS

Spring Boot（metric-service，D4–D5）
  指标/维度定义 → 自动生成 SQL → 查询服务（Redis 缓存、限流）
  演示用 Swagger + 截图，不做看板
```

| 天 | 内容 | 验收 |
|---|---|---|
| D1（本目录已做） | docker-compose 起 Kafka / ClickHouse / MySQL；订单生成器写 Kafka | 指定 QPS 写入，消费端按分区可见 |
| D2 | ClickHouse 星型模型 + ODS/DWD/DWS/ADS 分层 DDL | 能讲清每层职责 |
| D3 | 最小 Flink 作业 Kafka→ClickHouse；Hive/Spark 只学原理 | 端到端延迟实测 |
| D4 | 语义层：指标/维度定义 → 自动生成 SQL | 新增指标不改代码可查 |
| D5 | Redis 缓存 + 限流 + 压测 | P99 前后对比实测 |

分层和星型模型从 D2 才建表，M0 只把原始订单事件送到 Kafka topic `ods_order`。

## 模块

- `order-generator`：纯 `kafka-clients` 生产者（不引入 Spring），便于把 acks、key、幂等、批次这些概念看清楚。
- `metric-service`：Spring Boot 3 骨架，M0 只有 `GET /health`。语义层从 D4 开始做。

## 如何跑 M0

环境：JDK 17、Maven 3.9+、Docker（跑基础设施）。本机已有 Java / Maven 即可编译。

### 1. 启动 Kafka、ClickHouse、MySQL

```bash
docker compose up -d
docker compose ps    # 三个服务都 healthy 再往下
```

Kafka 对本机暴露 `localhost:9092`。ClickHouse（`8123` / `9000`）和 MySQL（`3306`，用户/密码均为 `metric`）D2 才会真正用到，M0 先起好。

没有 Docker 时，可以用 Apache Kafka 官方压缩包以 KRaft 单机模式启动，监听同样的 `localhost:9092`，生成器和消费者不用改。示例：

```bash
# 只需启动 Kafka 就能验证 M0；ClickHouse / MySQL 留给有 Docker 的环境
export KAFKA_HEAP_OPTS="-Xmx512m -Xms256m"
kafka-storage.sh format -t "$(kafka-storage.sh random-uuid)" -c config/kraft/server.properties
kafka-server-start.sh config/kraft/server.properties
```

### 2. 编译

```bash
mvn -DskipTests package
```

### 3. 按指定 QPS 写入模拟订单

```bash
# 100 条/秒，共 1000 条，topic 不存在时自动建成 3 个分区
java -jar order-generator/target/order-generator-0.1.0-SNAPSHOT.jar --qps 100 --count 1000 --partitions 3
```

常用参数（也可用环境变量，命令行优先，见 `Config.java`）：

| 参数 | 默认 | 含义 |
|---|---|---|
| `--bootstrap-servers` | `localhost:9092` | Kafka 地址 |
| `--topic` | `ods_order` | topic 名 |
| `--partitions` | `3` | 仅在新建 topic 时生效 |
| `--qps` | `100` | 目标发送速率 |
| `--count` | `1000` | 发送条数，`0` 表示一直发 |

消息 key 是 `shopId`（同一店铺进入同一分区），value 是订单 JSON：`orderId, shopId, skuId, channel, userId, quantity, amount, status, eventTime`。

### 4. 验证消费（按分区计数）

发送完成后再读，务必加 `--from-beginning`，并使用新的 consumer group（默认每次随机生成）：

```bash
java -cp order-generator/target/order-generator-0.1.0-SNAPSHOT.jar \
  com.jiawa.metric.generator.SimpleConsumer \
  --from-beginning --expect 1000 --idle-sec 8
```

也可以一条命令跑完「发送 + 按分区点数」：

```bash
./scripts/verify-m0.sh 100 1000
```

如果环境里有 Docker，也可以用自带的控制台消费者看一眼消息内容：

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ods_order --from-beginning --max-messages 5
```

### 5. 确认 metric-service 能启动（可选）

```bash
java -jar metric-service/target/metric-service-0.1.0-SNAPSHOT.jar
# 另开一个终端：curl http://localhost:8080/health
# 应返回 {"status":"UP"}
```

## M0 阅读顺序

按这个顺序看，可以把 Kafka 的几个核心概念串起来。每个文件都不长，建议对照注释逐行看。

1. **`README.md`、`计划.md`**  
   先建立全局图：为什么做这个项目、5 天各做什么、M0 停在哪一层（只把模拟订单送进 Kafka）。

2. **`docker-compose.yml`**  
   三个组件怎么起、端口怎么映射。重点看 Kafka 为什么要同时配 `localhost:9092` 和 `kafka:29092`（advertised.listeners：broker 会把这个地址告诉客户端，配错了就会出现「容器里通、本机不通」或反过来）。了解 **topic** 还没在这里创建，是生成器启动时用 AdminClient 显式建的。

3. **根目录 `pom.xml`，然后 `order-generator/pom.xml`、`metric-service/pom.xml`**  
   多模块如何统一 Java 17 和依赖版本；为什么生成器不用 Spring（避免把 Kafka 客户端配置藏进自动配置）。

4. **`Config.java`**  
   参数从哪来（命令行 > 环境变量 > 默认值），没有隐藏的配置文件。

5. **`OrderEvent.java`、`OrderEventFactory.java`**  
   消息正文长什么样；为什么金额用 `BigDecimal`；`eventTime` 是事件发生时间，后面 Flink 水位线会用到它。确认自己能讲清「这是模拟数据」。

6. **`KafkaTopicAdmin.java`**  
   **topic / partition**：topic 是逻辑分类，partition 是并行单位。为什么关闭自动建 topic、为什么默认 3 个分区。`AdminClient` 的用法。

7. **`OrderProducer.java`（本阶段最重要）**  
   对照注释看这几项，并能用自己的话讲出来：
   - **key**：这里用 `shopId` 分区，同一店铺有序、便于按店聚合；若用 `orderId` 则更均匀但会打散同一店铺。
   - **acks=all**：ISR 全部确认才算成功。
   - **幂等（enable.idempotence）**：同一生产者会话内，重试不会写出重复消息；进程重启后不能靠它跨会话去重。
   - **linger.ms / batch.size**：按分区分批发送，用一点点延迟换吞吐。
   - `send` 是异步的，统计必须在 callback 里做，结束前要 `flush`。

8. **`OrderGeneratorApp.java`**  
   主流程和限速方式（用「下一条的截止时间」而不是固定 sleep，避免误差累积）。跑完后看打印的「目标 QPS / 实测吞吐 / 各分区条数」。

9. **`SimpleConsumer.java`**  
   **consumer group**：同组内一条消息只被一个消费者拿到，位移按分区记录。为什么验证要用新的 group.id，以及 `--from-beginning` 时必须先等到分区分配再 `seekToBeginning`。跑完对照生产者的分区计数，两边应该一致。

10. **`MetricServiceApplication.java`、`HealthController.java`、`application.yml`**  
    扫一眼即可。M0 只证明 Spring Boot 服务能起，D4 再往里加语义层。

看完建议自己改两处再跑一次，加深印象：把 key 改成 `orderId`，观察分区分布有什么变化；把 `--qps` 调到 `20` 和 `500`，看 `linger.ms` 对实测吞吐的影响。
