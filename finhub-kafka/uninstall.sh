#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

for module in feed processor logger
do
  manifest="finhub-kafka-$module/target/kubernetes/minikube.yml"
  [[ -f $manifest ]] && kubectl delete -f "$manifest" --ignore-not-found
done

kubectl delete secret finnhub --ignore-not-found

kubectl delete -f k8s/kafka-ui.yaml --ignore-not-found
kubectl delete -f k8s/kafka-topics.yaml --ignore-not-found --timeout=120s
kubectl delete -f k8s/kafka-cluster.yaml --ignore-not-found --timeout=300s

helm uninstall strimzi --namespace strimzi --ignore-not-found

kubectl delete namespace kafka strimzi --ignore-not-found

kubectl delete crd -l app=strimzi --ignore-not-found
