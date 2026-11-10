-- MySQL 第一次启动时自动执行（挂在 /docker-entrypoint-initdb.d/）。
-- 库 metric 已由环境变量 MYSQL_DATABASE 创建。
-- D4 语义层会在这里放指标/维度定义表，M0 只先确认能连上。
SELECT 'metric database is ready' AS msg;
