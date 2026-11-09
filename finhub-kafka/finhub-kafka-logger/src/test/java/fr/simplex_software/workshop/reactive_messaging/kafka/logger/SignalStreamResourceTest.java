package fr.simplex_software.workshop.reactive_messaging.kafka.logger;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.quarkus.test.common.*;
import io.quarkus.test.common.http.*;
import io.quarkus.test.junit.*;
import io.quarkus.test.kafka.*;
import io.smallrye.reactive.messaging.kafka.companion.*;
import jakarta.inject.*;
import jakarta.json.bind.*;
import org.apache.kafka.clients.producer.*;
import org.junit.jupiter.api.*;

import java.io.*;
import java.math.*;
import java.net.*;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

@QuarkusTest
@WithTestResource(KafkaCompanionResource.class)
class SignalStreamResourceTest
{
  private static final Jsonb JSONB = JsonbBuilder.create();

  @InjectKafkaCompanion
  KafkaCompanion companion;

  @Inject
  SignalLogger signalLogger;

  @TestHTTPResource("/api/v1/signals/stream")
  URI streamUri;

  @Test
  void stream_signalPublished_pushesJsonEvent() throws Exception
  {
    try (SseClient client = new SseClient(streamUri))
    {
      publishUntilReceived("AAPL", client);

      assertThat(client.contentType()).startsWith("text/event-stream");
      PriceSignal signal = client.signalFor("AAPL");
      assertThat(signal.direction()).isEqualTo(Direction.UP);
      assertThat(signal.price()).isEqualByComparingTo("261.74");
    }
  }

  @Test
  void stream_twoClients_bothReceiveSignal()
  {
    try (SseClient first = new SseClient(streamUri); SseClient second = new SseClient(streamUri))
    {
      publishUntilReceived("MSFT", first, second);
    }
  }

  @Test
  void stream_malformedRecord_keepsStreamOpen()
  {
    try (SseClient client = new SseClient(streamUri))
    {
      publishUntilReceived("GOOG", client);

      publishRaw("this is not JSON");
      publishUntilReceived("AMZN", client);

      assertThat(client.signals()).allSatisfy(signal -> assertThat(signal.symbol()).isNotNull());
    }
  }

  @Test
  void stream_clientDisconnects_otherClientKeepsReceiving()
  {
    try (SseClient remaining = new SseClient(streamUri))
    {
      try (SseClient leaving = new SseClient(streamUri))
      {
        publishUntilReceived("NVDA", remaining, leaving);
      }
      publishUntilReceived("TSLA", remaining);
    }
  }

  @Test
  void signals_subscriberWithoutDemand_dropsAndContinues()
  {
    List<PriceSignal> received = new CopyOnWriteArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();
    signalLogger.signals().subscribe().with(subscription::set, received::add, failure::set, () -> {});

    signalLogger.onSignal(signal("DROPPED"));
    subscription.get().request(1);
    signalLogger.onSignal(signal("KEPT"));
    subscription.get().cancel();

    assertThat(failure.get()).isNull();
    assertThat(received).extracting(PriceSignal::symbol).containsExactly("KEPT");
  }

  private void publishUntilReceived(String symbol, SseClient... clients)
  {
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(() ->
    {
      publishRaw(JSONB.toJson(signal(symbol)));
      for (SseClient client : clients)
        assertThat(client.signals()).extracting(PriceSignal::symbol).contains(symbol);
    });
  }

  private void publishRaw(String value)
  {
    companion.produceStrings()
      .fromRecords(new ProducerRecord<>("signals", "test", value))
      .awaitCompletion();
  }

  private static PriceSignal signal(String symbol)
  {
    return new PriceSignal(symbol, new BigDecimal("261.74"), Direction.UP,
      new BigDecimal("2.03"), new BigDecimal("0.78"));
  }

  private static class SseClient implements AutoCloseable
  {
    private final HttpClient http = HttpClient.newHttpClient();
    private final List<PriceSignal> signals = new CopyOnWriteArrayList<>();
    private final CompletableFuture<HttpResponse<Stream<String>>> response;

    private SseClient(URI uri)
    {
      response = http.sendAsync(HttpRequest.newBuilder(uri).GET().build(),
        HttpResponse.BodyHandlers.ofLines());
      Thread.startVirtualThread(this::readEvents);
    }

    private void readEvents()
    {
      try
      {
        response.join().body()
          .filter(line -> line.startsWith("data:"))
          .map(line -> JSONB.fromJson(line.substring("data:".length()), PriceSignal.class))
          .forEach(signals::add);
      }
      catch (UncheckedIOException | CancellationException | CompletionException disconnected)
      {
      }
    }

    private List<PriceSignal> signals()
    {
      return signals;
    }

    private PriceSignal signalFor(String symbol)
    {
      return signals.stream().filter(signal -> symbol.equals(signal.symbol())).findFirst().orElseThrow();
    }

    private String contentType()
    {
      return response.join().headers().firstValue("Content-Type").orElseThrow();
    }

    @Override
    public void close()
    {
      if (!response.cancel(true))
        response.join().body().close();
      http.shutdownNow();
    }
  }
}
