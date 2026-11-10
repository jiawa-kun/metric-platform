#!/usr/bin/env bash
# M0 端到端验证：按指定 QPS 发送 N 条模拟订单，再用消费者按分区点数。
# 用法（在仓库根目录）：
#   ./scripts/verify-m0.sh
#   ./scripts/verify-m0.sh 100 1000    # QPS COUNT
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

QPS="${1:-100}"
COUNT="${2:-1000}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
JAR="$ROOT/order-generator/target/order-generator-0.1.0-SNAPSHOT.jar"

if [[ ! -f "$JAR" ]]; then
  echo "找不到 $JAR，请先在仓库根目录执行: mvn -DskipTests package"
  exit 1
fi

echo ">>> 发送 ${COUNT} 条，目标 QPS=${QPS}，bootstrap=${BOOTSTRAP}"
java -jar "$JAR" --bootstrap-servers "$BOOTSTRAP" --qps "$QPS" --count "$COUNT" --partitions 3

echo
echo ">>> 从 beginning 消费并按分区计数（期望 ${COUNT} 条）"
java -cp "$JAR" com.jiawa.metric.generator.SimpleConsumer \
  --bootstrap-servers "$BOOTSTRAP" \
  --from-beginning \
  --expect "$COUNT" \
  --idle-sec 8
