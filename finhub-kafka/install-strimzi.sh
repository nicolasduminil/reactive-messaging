#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -f .env ]]
then
  echo "Missing $(pwd)/.env — it must hold FINNHUB_TOKEN=<your-key>" >&2
  exit 1
fi

minikube_images=$(minikube image ls)

for module in feed processor logger
do
  manifest="finhub-kafka-$module/target/kubernetes/minikube.yml"
  if [[ ! -f $manifest ]]
  then
    echo "Missing $manifest — run 'mvn package' first" >&2
    exit 1
  fi
  image=$(awk '/^ +image: /{print $2; exit}' "$manifest")
  if ! printf '%s\n' "$minikube_images" | grep -qxF -e "$image" -e "${image#docker.io/}"
  then
    echo "Image $image is not in minikube's daemon. Build it there with:" >&2
    echo "  eval \$(minikube docker-env)" >&2
    echo "  mvn package -Dquarkus.container-image.build=true" >&2
    exit 1
  fi
done

kubectl create secret generic finnhub --from-env-file=.env \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create namespace strimzi --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -

helm repo add strimzi https://strimzi.io/charts
helm repo update strimzi

helm upgrade --install strimzi strimzi/strimzi-kafka-operator \
  --namespace strimzi \
  --set 'watchNamespaces={kafka}' \
  --wait --timeout 300s

kubectl apply -f k8s/kafka-cluster.yaml
kubectl apply -f k8s/kafka-topics.yaml
kubectl apply -f k8s/kafka-ui.yaml

kubectl wait kafka/finhub-cluster -n kafka --for=condition=Ready --timeout=600s

for module in feed processor logger
do
  kubectl apply -f "finhub-kafka-$module/target/kubernetes/minikube.yml"
done

echo
echo "Deployed. Watch the signals with:"
echo "  kubectl logs -f deploy/finhub-kafka-logger"
echo "Browse the topics with:"
echo "  minikube service kafka-ui -n kafka --url"
