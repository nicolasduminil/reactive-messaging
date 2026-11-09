#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

pids=finhub-kafka/.run-all.pids

if [[ -f $pids ]]
then
  while read -r pid
  do
    if ps -p "$pid" -o args= 2>/dev/null | grep -q quarkus-run.jar
    then
      kill "$pid"
    fi
  done < "$pids"
  for _ in $(seq 1 30)
  do
    alive=0
    while read -r pid
    do
      ps -p "$pid" >/dev/null 2>&1 && alive=1
    done < "$pids"
    [[ $alive == 0 ]] && break
    sleep 1
  done
  rm -f "$pids"
fi

docker rm -f kafka >/dev/null 2>&1 || true
echo "Applications and Kafka container stopped."
