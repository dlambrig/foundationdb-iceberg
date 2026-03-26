#!/usr/bin/env bash
set -euo pipefail

mkdir -p /var/fdb/data /var/fdb/logs /var/fdb
printf 'docker:docker@127.0.0.1:%s\n' "${FDB_PORT}" > /var/fdb/fdb.cluster

exec fdbserver \
  --listen-address 0.0.0.0:"${FDB_PORT}" \
  --public-address 127.0.0.1:"${FDB_PORT}" \
  --datadir /var/fdb/data \
  --logdir /var/fdb/logs \
  --locality-zoneid="$(hostname)" \
  --locality-machineid="$(hostname)" \
  --class "${FDB_PROCESS_CLASS:-unset}"
