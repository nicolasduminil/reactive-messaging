#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${FINNHUB_TOKEN:-}" && -f finhub-kafka/.env ]]
then
  set -a
  source finhub-kafka/.env
  set +a
fi
if [[ -z "${FINNHUB_TOKEN:-}" ]]
then
  echo "FINNHUB_TOKEN is not set, and finhub-kafka/.env does not provide it" >&2
  exit 1
fi

docker rm -f kafka >/dev/null 2>&1 || true
docker run -d --name kafka -p 9092:9092 apache/kafka-native:4.2.0
mvn package -DskipTests

# Background commands of a script ignore Ctrl+C, so the applications are stopped explicitly when the script ends.
# Their process IDs are also recorded, so that stop-all.sh can stop them from another terminal.
pids=finhub-kafka/.run-all.pids
trap 'kill $(jobs -p) 2>/dev/null; rm -f "$pids"' EXIT
: > "$pids"

port=8080
for m in logger processor feed; do
  java -Dquarkus.profile=prod -Dquarkus.http.port=$port \
    '-Dquarkus.log.category."io.smallrye.reactive.messaging".level=OFF' \
    '-Dquarkus.log.category."org.apache.kafka.clients.NetworkClient".level=OFF' \
    '-Dquarkus.log.category."org.jboss.resteasy.reactive.server.handlers.PublisherResponseHandler".level=OFF' \
    -jar finhub-kafka/finhub-kafka-$m/target/quarkus-app/quarkus-run.jar &
  echo $! >> "$pids"
  port=$((port + 1))
done
wait
