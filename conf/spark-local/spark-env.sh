# 本機 Spark 的環境變數（由 spark-submit 的 load-spark-env.sh 自動 source）。
# SPARK_LOCAL_IP=loopback：macOS 換網路後 hostname 常解析到過期 IP（DHCP 快取），
# driver 綁不上 → netty bind 失敗、所有 Spark 啟動秒炸。local 模式一律走 127.0.0.1
# 即可，完全避開 hostname 解析。（2026-07-07，見 known-pitfalls.md §7）
export SPARK_LOCAL_IP=127.0.0.1
