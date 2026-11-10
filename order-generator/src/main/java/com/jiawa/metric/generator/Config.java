package com.jiawa.metric.generator;

import java.util.HashMap;
import java.util.Map;

/**
 * 生成器和验证消费者共用的配置。
 *
 * <p>优先级：命令行参数 &gt; 环境变量 &gt; 下面的默认值。
 * 不引入任何外部配置库，解析过程可以逐行看完。
 *
 * <p>常用例子：
 * <pre>
 *   java -jar order-generator.jar --qps 100 --count 1000 --partitions 3
 *   java -cp order-generator.jar com.jiawa.metric.generator.SimpleConsumer --from-beginning --expect 1000
 * </pre>
 */
public final class Config {

    public final String bootstrapServers;
    public final String topic;
    /** 创建 topic 时使用的分区数。topic 已存在则不会改它的分区数。 */
    public final int partitions;
    /** 单机开发固定为 1。多 broker 集群才会大于 1。 */
    public final int replicationFactor;
    /** 目标发送速率：每秒多少条。 */
    public final int qps;
    /** 本次发送条数。0 表示一直发，直到 Ctrl+C。 */
    public final int count;
    /** 消费者连续多少秒收不到新消息就退出。 */
    public final int idleSec;
    /** 消费者期望收到的条数。达到后立刻退出；0 表示只靠 idleSec 退出。 */
    public final int expect;
    /** true 时从每个分区的最早消息开始读，适合发送后再验证。 */
    public final boolean fromBeginning;
    /** 消费者组 id。为空则 SimpleConsumer 自己生成一个，避免读到旧的提交位移。 */
    public final String groupId;

    private Config(String bootstrapServers, String topic, int partitions, int replicationFactor,
                   int qps, int count, int idleSec, int expect, boolean fromBeginning, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.qps = qps;
        this.count = count;
        this.idleSec = idleSec;
        this.expect = expect;
        this.fromBeginning = fromBeginning;
        this.groupId = groupId;
    }

    public static Config fromArgs(String[] args) {
        Map<String, String> cli = parseCli(args);
        if (cli.containsKey("help")) {
            printHelp();
            System.exit(0);
        }

        String bootstrap = first(cli.get("bootstrap-servers"), env("KAFKA_BOOTSTRAP_SERVERS"), "localhost:9092");
        String topic = first(cli.get("topic"), env("KAFKA_TOPIC"), "ods_order");
        int partitions = intVal(first(cli.get("partitions"), env("TOPIC_PARTITIONS"), "3"));
        int replication = intVal(first(cli.get("replication-factor"), env("TOPIC_REPLICATION_FACTOR"), "1"));
        int qps = intVal(first(cli.get("qps"), env("ORDER_QPS"), "100"));
        int count = intVal(first(cli.get("count"), env("ORDER_COUNT"), "1000"));
        int idleSec = intVal(first(cli.get("idle-sec"), env("CONSUMER_IDLE_SEC"), "8"));
        int expect = intVal(first(cli.get("expect"), env("CONSUMER_EXPECT"), "0"));
        boolean fromBeginning = flag(cli, "from-beginning")
                || "true".equalsIgnoreCase(env("CONSUMER_FROM_BEGINNING"));
        String groupId = first(cli.get("group-id"), env("CONSUMER_GROUP_ID"), "");

        if (partitions < 1 || qps < 1 || count < 0 || idleSec < 1 || replication < 1) {
            throw new IllegalArgumentException("partitions/qps/replication-factor/idle-sec 必须 >= 1，count 必须 >= 0");
        }
        return new Config(bootstrap, topic, partitions, replication, qps, count, idleSec, expect, fromBeginning, groupId);
    }

    public void logOnStart(String appName) {
        System.out.println("=== " + appName + " ===");
        System.out.println("bootstrapServers = " + bootstrapServers);
        System.out.println("topic            = " + topic);
        System.out.println("partitions       = " + partitions + "（仅在 topic 不存在时生效）");
        System.out.println("qps              = " + qps);
        System.out.println("count            = " + (count == 0 ? "无限" : count));
        System.out.println("说明：本程序写入的是模拟电商订单，不是真实业务数据。");
    }

    static void printHelp() {
        System.out.println("用法:");
        System.out.println("  java -jar order-generator.jar [选项]");
        System.out.println("  java -cp order-generator.jar com.jiawa.metric.generator.SimpleConsumer [选项]");
        System.out.println();
        System.out.println("选项:");
        System.out.println("  --bootstrap-servers  host:port   默认 localhost:9092，环境变量 KAFKA_BOOTSTRAP_SERVERS");
        System.out.println("  --topic              名称        默认 ods_order，环境变量 KAFKA_TOPIC");
        System.out.println("  --partitions         整数        默认 3，环境变量 TOPIC_PARTITIONS（只在新建 topic 时生效）");
        System.out.println("  --qps                整数        默认 100，环境变量 ORDER_QPS");
        System.out.println("  --count              整数        默认 1000，0 表示一直发，环境变量 ORDER_COUNT");
        System.out.println("  --from-beginning                 消费者从最早的消息读（验证用）");
        System.out.println("  --expect             整数        消费者收到这么多条就退出");
        System.out.println("  --idle-sec           整数        消费者连续多少秒没有新消息就退出，默认 8");
        System.out.println("  --group-id           字符串      消费者组。不传则每次生成一个新的");
        System.out.println("  --help");
    }

    /**
     * 把 ["--qps", "50", "--from-beginning"] 解析成 {"qps":"50", "from-beginning":"true"}。
     * 只认 --key value 或 --flag 两种形式。
     */
    static Map<String, String> parseCli(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("无法识别的参数: " + arg + "（参数必须以 -- 开头）");
            }
            String key = arg.substring(2);
            boolean hasValue = i + 1 < args.length && !args[i + 1].startsWith("--");
            if (hasValue) {
                map.put(key, args[++i]);
            } else {
                map.put(key, "true");
            }
        }
        return map;
    }

    private static boolean flag(Map<String, String> cli, String key) {
        return "true".equalsIgnoreCase(cli.getOrDefault(key, "false"));
    }

    private static String env(String name) {
        return System.getenv(name);
    }

    /** 返回第一个非空字符串。 */
    private static String first(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        return "";
    }

    private static int intVal(String raw) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("不是整数: " + raw);
        }
    }
}
