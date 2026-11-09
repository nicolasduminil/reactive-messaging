# reactive-messaging — a four-stage Quarkus messaging workshop

**Quarkus 3.x** applications demonstrating **MicroProfile / SmallRye Reactive Messaging**: poll
live stock quotes from the [Finnhub](https://finnhub.io) API, classify every price movement, and
log the result — a three-role reactive pipeline wired by channel name.

The workshop advances in four stages, each a runnable module set:

| Stage | Module(s) | What travels | Where the channels live |
|-------|-----------|--------------|-------------------------|
| 1 | `finhub-simple` | bare `Quote` payloads | in-memory, one JVM |
| 2 | `finhub-msg` | `Message<Quote>` envelopes with ack / nack | in-memory, one JVM |
| 3 | `finhub-kafka/*` | JSON over Kafka topics | three JVMs, a real broker |
| 4 | `finhub-kafka-alert` | JSON over Kafka topics, consumed on virtual threads | a fourth JVM, the same broker |

The comparison between Mutiny and virtual threads on HTTP endpoints, with its load measurement, formerly the
stage 4 of this project, now resides in the project
[`mutiny-virtual-threads`](https://github.com/nicolasduminil/mutiny-virtual-threads).

---

## Architecture

The same three canonical roles — **producer → processor → consumer** — appear in stages 1 to 3,
connected by two channels, `quotes` and `signals`:

```
                @Outgoing("quotes")                @Outgoing("signals")
  ┌────────────┐   Quote   ┌───────────────┐  PriceSignal  ┌───────────────────┐
  │ QuoteFeed  │─────────▶ │ QuotePipeline │ ────────────▶ │ QuotePipeline     │
  │ (producer) │           │  (processor)  │               │   (consumer)      │
  └────────────┘           └───────────────┘               └───────────────────┘
   polls Finnhub            UP / DOWN / FLAT vs. the          logs + retains the
   every N seconds          previously seen price              latest signal
```

### Stages 1 and 2 — in-memory, one JVM

With no connector configured, SmallRye wires each `@Outgoing` to the matching `@Incoming` *within
the same JVM*. There is no broker. The wiring is by channel **name**, not by class, so the roles
can live in any module and still connect.

Only the producer differs between the two applications:

| Application | Producer | Emits |
|-------------|----------|-------|
| `finhub-simple` | `QuoteFeed` | `Multi<Quote>` — the bare payload |
| `finhub-msg` | `QuoteFeedWithMsg` | `Multi<Message<Quote>>` — a `QuoteMessage` envelope |

The processor and consumer bindings they share live once, in `finhub-pipeline`.

### Stage 3 — Kafka, three JVMs

`finhub-kafka` is an aggregator over three independently deployable applications. Each owns
**exactly one** side of the topology and meets the others only through a topic:

```
  finhub-kafka-feed          topic         finhub-kafka-processor        topic       finhub-kafka-logger
  ┌────────────────┐        quotes        ┌──────────────────────┐      signals      ┌─────────────────┐
  │ QuoteFeedToKafka│ ──────────────────▶ │    QuoteProcessor    │ ────────────────▶ │  SignalLogger   │
  │ @Outgoing       │                     │ @Incoming @Outgoing  │                   │  @Incoming      │
  └────────────────┘                      └──────────────────────┘                   └─────────────────┘
```

Channel name equals topic name throughout, so no `topic=` override is needed anywhere. A fourth pod,
`kafka-ui`, sits beside the broker and reads both topics — see [The Kafka GUI](#the-kafka-gui).

The processor and the logger log and skip a malformed record, then continue with the next one. This
behaviour requires two channel properties: `fail-on-deserialization-failure=false` forwards a null
payload instead of terminating the poll, and `failure-strategy=ignore` tolerates the resulting nack.
A skipped record remains on its topic, where the Kafka GUI displays it.

`finhub-kafka-logger` additionally publishes each signal to HTTP clients. See
[The signal stream](#the-signal-stream).

### Stage 4 — a consumer on virtual threads

The services of stage 3 never block, so virtual threads would bring them nothing. `finhub-kafka-alert` is a fourth
service that does block: for each `UP` or `DOWN` signal, it calls a webhook through a blocking REST client. It reads the
existing `signals` topic under its own consumer group, and the other services remain unchanged. See
[The alert consumer](#the-alert-consumer).

### Why `finhub-common` declares no channel bindings

`finhub-common` carries a **Jandex index** (`jandex-maven-plugin`) — without one Quarkus does not
scan the dependency and none of its beans, not even the `@RegisterRestClient` interface, are
discovered. The consequence: **every** bean in it activates in **every** module that depends on it.

An `@Incoming`/`@Outgoing` bean there would therefore run in all six applications at once, which
is precisely what makes a per-role deployment impossible. So `finhub-common` holds only
annotation-free collaborators (`QuoteSource`, `PriceMovementEvaluator`, `PriceSignalLogger`), and
**channel bindings are owned by the deployment that runs them** — `finhub-pipeline` for the
single-JVM stages, one thin class per module for the Kafka stage.

### Data model (all monetary values are `BigDecimal`)

| Type | Fields | Notes |
|------|--------|-------|
| `Quote` | `symbol`, `price`, `previousClose` | Domain payload on the `quotes` channel |
| `PriceSignal` | `symbol`, `price`, `direction`, `change`, `changePercent` | Payload on the `signals` channel |
| `Direction` | `UP`, `DOWN`, `FLAT` | Sign of the price move |
| `FinnhubQuote` | `current` (`c`), `previousClose` (`pc`) | REST DTO; JSON-B–mapped, absent fields default to `0` |
| `QuoteMessage` | wraps a `Quote` | `Message<Quote>` envelope logging ack / nack |

Prices use `BigDecimal` (not `double`) for exact decimal arithmetic and safe comparison. The
evaluator computes direction via `change.signum()` and the percentage as
`change.movePointRight(2).divide(reference, 2, HALF_UP)` — a single, explicit rounding step.

### Message envelopes (`finhub-msg`)

A payload-returning method lets the framework create the envelope; returning `Message` yourself
adds metadata and the two settlement callbacks. `QuoteMessage` logs both:

```
Acknowledgment for Quote[symbol=AAPL, price=261.74, previousClose=259.71]
```

The callbacks must **complete the returned stage themselves**. Delegating to
`Message.super.ack()` recurses forever, because the default `ack()` is specified as "invoke the
supplier returned by `getAck()`" — that is, the very callback being written. `QuoteMessageTest`
pins this down.

---

## Requirements

- **JDK 21**
- **Maven 3.9+**
- A free **Finnhub API key** ([finnhub.io](https://finnhub.io)) to run against the live API
- **Docker** — required for the Kafka modules (Dev Services / Testcontainers)
- **minikube**, **kubectl** and **helm** — only to deploy stage 3 on Kubernetes

---

## Project layout

```
reactive-messaging/                 parent POM (fr.simplex-software.workshop:reactive-messaging)
├── finhub-common/                   shared library — NO @Incoming/@Outgoing (see above)
│   └── src/main/java/…/reactive_messaging/
│       ├── client/                       FinnhubClient (REST client) + FinnhubQuote DTO
│       ├── domain/                       Quote, PriceSignal, Direction, QuoteMessage
│       ├── feed/QuoteSource.java         the Finnhub polling stream, unbound to any channel
│       ├── feed/FinnhubConfig.java       the type-safe finnhub.* configuration
│       └── signal/
│           ├── PriceMovementEvaluator.java   quote -> signal, plain logic
│           └── PriceSignalLogger.java        formats + retains the latest signal
├── finhub-pipeline/                 the in-JVM topology, for stages 1 and 2 only
│   └── …/pipeline/QuotePipeline.java     @Incoming("quotes") @Outgoing("signals") + @Incoming("signals")
├── finhub-simple/                   stage 1 — publishes bare Quote payloads
│   └── …/QuoteFeed.java                  producer @Outgoing("quotes")
├── finhub-msg/                      stage 2 — publishes Message envelopes
│   └── …/QuoteFeedWithMsg.java           producer @Outgoing("quotes")
├── finhub-kafka/                    stage 3 — aggregator POM, no code of its own
│   ├── install-strimzi.sh                one-shot deployment (operator → cluster → GUI → apps)
│   ├── uninstall.sh                      tears it all back down, CRDs included
│   ├── k8s/                              hand-written manifests (see below)
│   │   ├── kafka-cluster.yaml                 Strimzi KafkaNodePool + Kafka
│   │   ├── kafka-topics.yaml                  KafkaTopic quotes, signals, dead-letter-topic-signals
│   │   └── kafka-ui.yaml                      kafbat/kafka-ui — the topic browser
│   ├── finhub-kafka-feed/                …/kafka/feed/QuoteFeedToKafka.java
│   ├── finhub-kafka-processor/           …/kafka/processor/QuoteProcessor.java + QuoteDeserializer
│   ├── finhub-kafka-logger/              …/kafka/logger/SignalLogger.java + SignalStreamResource + PriceSignalDeserializer
│   └── finhub-kafka-alert/               stage 4 — …/kafka/alert/SignalAlerter.java + AlertClient + PriceSignalDeserializer
├── .circleci/config.yml             CI: mvn verify on a machine executor (Docker needed)
└── README.md                        this file
```

---

## Configuration

Each application has its own `src/main/resources/application.properties` — Quarkus does not pick
config up from dependency jars.

The modules that call Finnhub (`finhub-simple`, `finhub-msg` and `finhub-kafka-feed`) read:

| Variable | Default | Purpose |
|----------|---------|---------|
| `FINNHUB_TOKEN` | *(empty)* | Finnhub API key — **required** to fetch real quotes |
| `FINNHUB_SYMBOL` | `AAPL` | Ticker symbol to follow |
| `FINNHUB_INTERVAL` | `15` | Seconds between polls (mind free-tier rate limits) |

The REST client base URL is fixed to `https://finnhub.io/api/v1`. If `FINNHUB_TOKEN` is absent the
app still starts; each fetch fails auth and is logged-and-skipped rather than crashing the stream.

`finhub-kafka-alert` reads:

| Variable | Default | Purpose |
|----------|---------|---------|
| `ALERT_URL` | `http://localhost:9000/alerts` | The webhook receiving each alert |
| `ALERT_MAX_CONCURRENCY` | `32` | Concurrent webhook calls |
| `ALERT_POOL` | the value of `ALERT_MAX_CONCURRENCY`, and `4096` in the test profile | Size of the connection pool |

The burst test also reads `ALERT_BURST` (default `64`) and `ALERT_DELAY_MS` (default `500`), and it logs the values used,
the peak and the duration, so each limit can be measured without modifying any file:

```bash
ALERT_MAX_CONCURRENCY=1024 ALERT_BURST=2048 ALERT_DELAY_MS=2000 \
  mvn test -pl finhub-kafka/finhub-kafka-alert -am \
  -Dtest='SignalAlerterTest#alert_burst_overlapsWithinConcurrencyLimit' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The measurement is a single log line, for example
`Burst of 2048 signals, limit 1024, pool 4096, delay 2000 ms: peak of 1024 concurrent calls, 4867 ms`. Append
`| grep "Burst of"` to the command to display it alone, which also hides the diagnosis of a failure, or extract it from
the test report after the build:

```shell
grep -o "Burst of[^&<]*" \
  finhub-kafka/finhub-kafka-alert/target/surefire-reports/TEST-fr.simplex_software.workshop.reactive_messaging.kafka.alert.SignalAlerterTest.xml
```

The Kafka modules read `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) — **in the `prod`
profile only**. Dev and test get a broker from Dev Services automatically.

---

## Running

### Stages 1 and 2 — dev mode (live reload)

```shell
FINNHUB_TOKEN=<your-key> mvn -pl finhub-simple -am quarkus:dev
```

Swap `finhub-simple` for `finhub-msg` to run the envelope-based variant. The log emits one line
per poll:

```
AAPL 261.74 USD ▲ +2.03 (+0.78%)
AAPL 260.00 USD ▼ -1.74 (-0.66%)
```

### Stage 3 — locally

In dev mode each module starts its own Dev Services broker, which is *not* shared, so the three services run in the
`prod` profile against one broker instead. `run-all.sh` does the whole sequence:

```shell
./finhub-kafka/run-all.sh
```

It works from any directory and:

1. takes `FINNHUB_TOKEN` from the environment, or else from `finhub-kafka/.env` (the file the minikube deployment uses),
   and stops if neither provides it;
2. starts a Kafka broker in a container named `kafka`, replacing any previous one;
3. builds the project without running the tests;
4. starts the logger, the processor and the feed on ports `8080`, `8081` and `8082`, and records their process IDs in
   `finhub-kafka/.run-all.pids` (gitignored).

The script stays in the foreground. Ctrl+C stops the three services and leaves the broker running. From any terminal,
`stop-all.sh` stops the recorded services and removes the broker:

```shell
./finhub-kafka/stop-all.sh
```

The feed polls Finnhub every 15 seconds, so the first signal can take that long. While the market is closed the price
does not move, and the signals are `FLAT` with a change of zero.

### Stage 3 — minikube with Strimzi

```shell
minikube start

# 1. build the images into minikube's own daemon (no registry involved)
eval $(minikube docker-env)
mvn package -Dquarkus.container-image.build=true

cd finhub-kafka

# 2. the API key — the 'finnhub' Secret is derived from this file (gitignored)
echo 'FINNHUB_TOKEN=<your-key>' > .env

# 3. operator, cluster, topics, GUI and the three applications
./install-strimzi.sh

kubectl logs -f deploy/finhub-kafka-logger
minikube service kafka-ui -n kafka --url     # the topic browser

./uninstall.sh                               # when you are done
```

`install-strimzi.sh` is idempotent — every step is a create-or-update — and ordered, which is the
part that matters:

1. **a pre-flight check** that every generated `minikube.yml` exists and that the image it names is
   in **minikube's** daemon. `imagePullPolicy` is `IfNotPresent`, so an image built against the host
   daemon instead leaves the kubelet falling back to Docker Hub and the pod in `ImagePullBackOff`;
   failing here prints the `eval $(minikube docker-env)` fix rather than letting you discover it from
   a pod event later.
2. the `strimzi` and `kafka` namespaces, and the `finnhub` Secret from `.env`
   (`kubectl create secret --from-env-file`, so rotating the key is just an edit of that file — the
   token never appears in a manifest).
3. the operator, `helm upgrade --install --wait`, watching `kafka` only. `--wait` is what guarantees
   the CRDs exist before the next step needs them.
4. the cluster, the topics and the GUI.
5. **`kubectl wait kafka/finhub-cluster --for=condition=Ready`** — without it the applications start
   against a broker that is not listening yet.
6. the three applications. The script applies their manifests without waiting for the pods, so it can exit 0 while a
   pod is still starting or failing: check with `kubectl get pods` or `kubectl logs -f deploy/finhub-kafka-logger`.

`uninstall.sh` reverses it, and the order there matters too: the topic operator puts a finalizer on
every `KafkaTopic`, so the custom resources must be deleted while the operator is still running —
uninstall the release first and the delete blocks forever. It also drops the CRDs, which the chart
installs but the release does not own, so the next install starts from a genuinely clean cluster.

The application `Deployment` and `Service` manifests are **generated** by Dekorate (the
`quarkus-minikube` extension) from `quarkus.kubernetes.*` properties — including a pinned
`quarkus.kubernetes.node-port` per module (30081–30084), since left unset the extension draws a
fresh random port on every build. The Strimzi `Kafka`, `KafkaNodePool` and `KafkaTopic` resources in
`k8s/` are hand-written: Dekorate models a fixed set of resource kinds and cannot emit third-party
custom resources. `install-strimzi.sh` deploys the three applications of stage 3 and not the alert consumer, because
the cluster contains no webhook to call.

### The signal stream

`GET /api/v1/signals/stream` on `finhub-kafka-logger` returns a `Multi<PriceSignal>` as server sent
events, each one carrying a JSON signal. With the local setup above, the logger listens on port
`8080`:

```shell
curl -N http://localhost:8080/api/v1/signals/stream
```

```
data:{"change":0.36,"changePercent":0.14,"direction":"UP","price":262.1,"symbol":"AAPL"}
```

The behaviour of this endpoint rests on three properties:

1. The stream is hot. A `BroadcastProcessor` in `SignalLogger` distributes each signal to the clients
   connected at that moment, so a client receives the signals published after its connection and
   earlier ones remain available through the Kafka GUI.
2. Logging and consumption proceed independently of the clients. The consumer logs each signal and
   commits its offset whether zero or many clients are connected.
3. Each client has an explicit back pressure policy, `onOverflow().drop()`. A client that reads too
   slowly misses signals and stays connected, because a newer price supersedes an older one. Without
   this policy, `BroadcastProcessor` terminates a client that has no outstanding demand.

The endpoint requires no authentication, which suits this workshop and excludes production use as it
stands.

### The alert consumer

```shell
mvn package -DskipTests
ALERT_URL=http://localhost:9000/alerts java -Dquarkus.profile=prod -Dquarkus.http.port=8083 \
  -jar finhub-kafka/finhub-kafka-alert/target/quarkus-app/quarkus-run.jar
```

```java
@Incoming("signals")
@RunOnVirtualThread
@Blocking("alerts")
public void alert(PriceSignal signal)
{
  if (signal.direction() != Direction.FLAT)
    client.send(signal);
}
```

`@RunOnVirtualThread` starts a virtual thread per message, and `@Blocking("alerts")` names the worker carrying the
concurrency limit. The following table reports single runs on one machine, with a webhook answering after a fixed delay:

| Consumer annotations | Maximum of concurrent virtual threads | Connection pool | Signals | Webhook delay | Peak of concurrent calls | Total duration |
|---|---|---|---|---|---|---|
| `@Blocking` | not applicable, sequential processing on a worker thread | 4096 | 64 | 500 ms | 1 | 32.39 s |
| `@RunOnVirtualThread`, `@Blocking("alerts")` | 4 | 4096 | 64 | 500 ms | 4 | 8.19 s |
| `@RunOnVirtualThread`, `@Blocking("alerts")` | 8 | 4096 | 64 | 500 ms | 8 | 4.14 s |
| `@RunOnVirtualThread`, `@Blocking("alerts")` | 32 | 4096 | 64 | 500 ms | 32 | 1.13 s |
| `@RunOnVirtualThread`, `@Blocking("alerts")` | 32 | 32 | 64 | 500 ms | 32 | 1.13 s |
| `@RunOnVirtualThread`, `@Blocking("alerts")` | 32 | 8 | 64 | 500 ms | 8 | 4.12 s |
| `@RunOnVirtualThread`, `@Blocking("alerts")` | 1024 | 50 | 200 | 500 ms | 50 | 2.17 s |
| `@RunOnVirtualThread`, `@Blocking("alerts")` | 1024 | 50 | 512 | 500 ms | not logged, since 12 alerts fail | not logged |
| `@RunOnVirtualThread`, `@Blocking("alerts")` | 1024 | 50, with a timeout of 9 s | 512 | 500 ms | 50 | 5.66 s |
| `@RunOnVirtualThread`, `@Blocking("alerts")` | 1024 | 4096 | 2048 | 2 s | 1024 | 5.01 s |
| `@RunOnVirtualThread` | 1024, the default | 4096 | 2048 | 2 s | 1024 | 5.01 s |

The behaviour of this service rests on three properties:

1. Processing is concurrent and abandons the order of the topic, even with one partition. A receiver must rely on the
   content of each alert rather than on its position. `QuoteProcessor` cannot adopt this mode, because its evaluator
   compares each quote with the previous one.
2. Virtual threads remove the bound of a worker pool, so `ALERT_MAX_CONCURRENCY` limits the load on the webhook. It
   also sizes the connection pool, whose default size of 50 would otherwise impose a lower limit of its own. A call
   awaiting a free connection remains subject to `read-timeout`, and it fails once this period of 5 s has elapsed.
3. A failed alert and a malformed record both reach `dead-letter-topic-signals`, the latter with its original content,
   and the consumer continues. The serializer of this topic is named explicitly, because the connector otherwise
   searches for a `PriceSignalSerializer` and the application refuses to start.

When a dead lettered record is the last one on the topic, the consumer group reports a lag of 1 until a further record
has been processed. A restart at that point redelivered nothing, and the delivery remains at least once, so the webhook
should tolerate a duplicate.

### The Kafka GUI

`k8s/kafka-ui.yaml` runs [kafbat/kafka-ui](https://github.com/kafbat/kafka-ui) — the maintained
continuation of provectus/kafka-ui — in the `kafka` namespace, so it addresses the broker as
`finhub-cluster-kafka-bootstrap:9092`. It is a `NodePort` Service on **30080**:

```shell
minikube service kafka-ui -n kafka --url
```

It answers the question the logger cannot: whether the topics exist, whether the feed produced,
whether the processor's consumer group is lagging, and what the JSON on the wire actually looks
like at each hop.

---

## Testing

```shell
mvn verify
```

Docker must be running — the Kafka modules start a real broker through Dev Services.

The log contains errors and warnings, some with a stack trace, although every test passes. The negative tests produce
them on purpose: a quote without a price, a malformed record, an alert refused by the webhook and a burst exceeding the
connection pool are each logged, rejected and followed by the next message. The Kafka clients add a few
`UNKNOWN_TOPIC_OR_PARTITION` warnings while each topic is created on first use. The result of the build is given by the
test counts and `BUILD SUCCESS`. To run the build without these messages, activate the `quiet` profile, which turns
their logging categories off (they are listed in `.mvn/quiet-logging.properties`):

```shell
mvn verify -Pquiet
```

This also hides any genuine error from these categories, so leave the profile out when investigating a failure.

| Test | Module | Type | Covers |
|------|--------|------|--------|
| `FinnhubQuoteTest` | `finhub-common` | plain JUnit | JSON-B mapping of the Finnhub response (`c`/`pc`), absent-field defaulting to zero |
| `QuoteSourceTest` | `finhub-common` | plain JUnit | A failed Finnhub fetch is skipped and the stream continues with the next poll |
| `PriceMovementEvaluatorTest` | `finhub-common` | plain JUnit | Direction logic — `UP` / `DOWN` / `FLAT`, first-quote-vs-previous-close, zero reference, separate history per symbol |
| `QuoteMessageTest` | `finhub-common` | plain JUnit | Envelope settlement — ack and nack complete instead of recursing, `null` nack reason rejected |
| `QuotePipelineTest` | `finhub-simple` | `@QuarkusTest` | Bare payloads reach the pipeline, are acknowledged, and produce the expected signal. An invalid quote is nacked |
| `QuoteMessagePipelineTest` | `finhub-msg` | `@QuarkusTest` | `Message<Quote>` envelopes produce the expected signal and are acknowledged. An invalid quote is nacked |
| `QuoteFeedToKafkaTest` | `finhub-kafka-feed` | `@QuarkusTest` + Dev Services | Quotes are JSON-serialised onto the `quotes` topic |
| `QuoteProcessorTest` | `finhub-kafka-processor` | `@QuarkusTest` + Dev Services | Black-box: JSON quotes in on `quotes`, JSON signals out on `signals`. A malformed record is skipped |
| `SignalLoggerTest` | `finhub-kafka-logger` | `@QuarkusTest` + Dev Services | Signals read off the `signals` topic reach the logger. A malformed record is skipped |
| `SignalStreamResourceTest` | `finhub-kafka-logger` | `@QuarkusTest` + Dev Services | Signals reach every connected client as JSON events. The stream survives a malformed record and another client's disconnection, and a client without demand drops signals |
| `SignalAlerterTest` | `finhub-kafka-alert` | `@QuarkusTest` + Dev Services + WireMock | `UP` and `DOWN` signals reach the webhook and `FLAT` signals do not. Calls run on virtual threads without pinning, reach the concurrency limit exactly and lose their order. A failed alert and a malformed record reach the dead letter topic |
| `SignalAlerterPoolTest` | `finhub-kafka-alert` | `@QuarkusTest` + Dev Services + WireMock | A pool of 8 under a limit of 32 caps the peak at 8 without failure. A call waiting longer than `read-timeout` for a connection reaches the dead letter topic |

The stage 1 and 2 `@QuarkusTest`s disable the producer under the `test` profile
(`@UnlessBuildProfile("test")`) and feed deterministic quotes into the `quotes` channel through
`smallrye-reactive-messaging-in-memory`. The stage 3 and 4 tests use `KafkaCompanion` to produce and
consume on real topics, so each module is exercised through its actual wire contract.

---

## Continuous integration

`.circleci/config.yml` runs `mvn -B -ntp verify` on a **machine executor**, which provides the
Docker daemon the Testcontainers-backed Kafka tests require, and caches the Maven repository
against a hash of every POM in the reactor.

---

## Conventions & versioning

- Group ID / base package: `fr.simplex-software.workshop`
- Maven multi-module layout with a parent POM; shared code in `finhub-common`
- Semantic versioning; PATCH bumped per generated version — currently `1.0.14-SNAPSHOT`
- No Lombok; integration-style tests preferred
