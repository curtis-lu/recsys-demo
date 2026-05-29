#!/usr/bin/env bash
# Run a one-shot PySpark admin/setup script against dev-cluster, fast.
#
# Pattern from ~/dev-cluster/README.md ("three ways to run a Spark job", method B)
# blockquote (line 77-91): transient devcluster/pyspark container with local[N]
# master and dynamicAllocation disabled — avoids the multi-minute standalone
# scheduler init that host-venv mode incurs. Bind-mounts the project root to
# /workspace so the script can read host parquet files at /workspace/...
#
# Usage:
#   scripts/dev_admin.sh scripts/<your_script>.py [args...]
#
# Why not host venv (.venv/bin/python ...) for these scripts:
#   1. Standalone master + dynamic allocation init takes ~3 min for a few-second
#      RPC.
#   2. host driver assigns file://<host path> to spark-worker container, which
#      can't see host fs (SOP-5 in dev-cluster-spark skill).
#   3. Some metastore RPCs hit the metastore container's fs.defaultFS which
#      resolves localhost to the metastore itself (SOP-4).
#
# Why not docker exec into devcluster-spark-master:
#   apache/spark image is JVM-only, no python3, no /workspace mount.

set -euo pipefail

SCRIPT="${1:?usage: $0 scripts/<your_script>.py [args...]}"
shift

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DC_CONF="${HOME}/dev-cluster/conf"

if [[ ! -d "$DC_CONF" ]]; then
  echo "[dev_admin] dev-cluster conf dir not found: $DC_CONF" >&2
  exit 1
fi

# Path inside the container: project root is mounted at /workspace.
# Accept either "scripts/foo.py" (relative to project root) or
# "/workspace/scripts/foo.py" (already container-side).
case "$SCRIPT" in
  /workspace/*) IN_CONTAINER_SCRIPT="$SCRIPT" ;;
  *)            IN_CONTAINER_SCRIPT="/workspace/$SCRIPT" ;;
esac

exec docker run --rm --network devcluster-net \
  -v "$DC_CONF/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro" \
  -v "$DC_CONF/spark/hive-site.xml:/opt/spark/conf/hive-site.xml:ro" \
  -v "$DC_CONF/hadoop/core-site.xml:/opt/spark/conf/core-site.xml:ro" \
  -v "$DC_CONF/hadoop/hdfs-site.xml:/opt/spark/conf/hdfs-site.xml:ro" \
  -v "$PROJECT_ROOT:/workspace" \
  --user root \
  devcluster/pyspark:v3.3.2 \
  /opt/spark/bin/spark-submit \
    --master "local[2]" \
    --conf spark.dynamicAllocation.enabled=false \
    "$IN_CONTAINER_SCRIPT" "$@"
