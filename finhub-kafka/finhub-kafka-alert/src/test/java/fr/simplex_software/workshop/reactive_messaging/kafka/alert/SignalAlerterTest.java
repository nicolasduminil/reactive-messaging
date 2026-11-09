package fr.simplex_software.workshop.reactive_messaging.kafka.alert;

import com.github.tomakehurst.wiremock.matching.*;
import com.github.tomakehurst.wiremock.stubbing.*;
import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.quarkus.test.common.*;
import io.quarkus.test.junit.*;
import io.quarkus.test.junit.virtual.*;
import io.quarkus.test.kafka.*;
import io.smallrye.reactive.messaging.kafka.companion.*;
import org.apache.kafka.clients.producer.*;
import org.eclipse.microprofile.config.inject.*;
import org.jboss.logging.*;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static fr.simplex_software.workshop.reactive_messaging.kafka.alert.AlertBurst.*;
import static fr.simplex_software.workshop.reactive_messaging.kafka.alert.WireMockWebhook.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

@QuarkusTest
@VirtualThreadUnit
@WithTestResource(KafkaCompanionResource.class)
@WithTestResource(WireMockWebhook.class)
class SignalAlerterTest
{
  private static final Logger LOG = Logger.getLogger(SignalAlerterTest.class);
  @InjectKafkaCompanion
  KafkaCompanion companion;
  @ConfigProperty(name = "smallrye.messaging.worker.alerts.max-concurrency")
  int limit;
  @ConfigProperty(name = "quarkus.rest-client.alert.connection-pool-size")
  int pool;
  @ConfigProperty(name = "alert.burst", defaultValue = "64")
  int burst;
  @ConfigProperty(name = "alert.delay-ms", defaultValue = "500")
  int delayMs;

  @BeforeEach
  void reset()
  {
    resetWebhook();
  }

  @Test
  void alert_upOrDownSignal_postsToWebhook()
  {
    publish("UPDN", Direction.UP);
    publish("UPDN", Direction.DOWN);

    awaitAlerts("UPDN", 2);

    SERVER.verify(postedFor("UPDN").withRequestBody(matchingJsonPath("$.direction", equalTo("UP"))));
    SERVER.verify(postedFor("UPDN").withRequestBody(matchingJsonPath("$.direction", equalTo("DOWN"))));
  }

  @Test
  void alert_flatSignal_sendsNothing()
  {
    publish("FLAT", Direction.FLAT);
    publish("AFTERFLAT", Direction.UP);
    awaitAlerts("AFTERFLAT", 1);
    await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2)).until(() -> alerts("FLAT").isEmpty());
  }

  @Test
  @ShouldNotPin
  void alert_signal_callsWebhookOnVirtualThreadWithoutPinning()
  {
    publish("THREAD", Direction.UP);
    awaitAlerts("THREAD", 1);
    SERVER.verify(postedFor("THREAD").withHeader(CallerThreadFilter.HEADER, equalTo("virtual")));
  }

  @Test
  void alert_burst_overlapsWithinConcurrencyLimit()
  {
    int expected = Math.min(Math.min(limit, pool), burst);
    publishDelayed(companion, "BURST", burst, delayMs);
    List<Long> starts = awaitStarts("BURST", burst, delayMs, expected);
    int peak = peakOverlap(starts, delayMs);
    LOG.infof("Burst of %d signals, limit %d, pool %d, delay %d ms: peak of %d concurrent calls, %d ms",
      burst, limit, pool, delayMs, peak, starts.getLast() + delayMs - starts.getFirst());
    awaitCompletion(starts, delayMs);
    assertThat(peak).isEqualTo(expected);
  }

  @Test
  void alert_slowEarlierSignal_completesAfterLaterOne()
  {
    SERVER.stubFor(post(anyUrl()).withRequestBody(matchingJsonPath("$.symbol", equalTo("SLOW")))
      .willReturn(aResponse().withStatus(204).withFixedDelay(2000)));
    publish("SLOW", Direction.UP);
    publish("FAST", Direction.UP);
    long slowEnd = awaitAlerts("SLOW", 1).getFirst().getRequest().getLoggedDate().getTime() + 2000;
    long fastEnd = awaitAlerts("FAST", 1).getFirst().getRequest().getLoggedDate().getTime();
    assertThat(fastEnd).isLessThan(slowEnd);
    await().atMost(Duration.ofSeconds(5)).until(() -> System.currentTimeMillis() > slowEnd);
  }

  @Test
  void alert_webhookFailure_routesToDeadLetterTopic()
  {
    SERVER.stubFor(post(anyUrl()).withRequestBody(matchingJsonPath("$.symbol", equalTo("REFUSED")))
      .willReturn(aResponse().withStatus(500)));
    publish("REFUSED", Direction.UP);
    assertThat(awaitDeadLetter("REFUSED")).contains("\"direction\":\"UP\"");
    publish("AFTERFAILURE", Direction.UP);
    awaitAlerts("AFTERFAILURE", 1);
  }

  @Test
  void alert_malformedRecord_routesToDeadLetterTopicAndProcessesNext()
  {
    publishRaw("this is not JSON");
    publish("AFTERMALFORMED", Direction.DOWN);
    awaitAlerts("AFTERMALFORMED", 1);
    assertThat(awaitDeadLetter("this is not JSON")).isEqualTo("this is not JSON");
  }

  private String awaitDeadLetter(String symbol)
  {
    try (ConsumerTask<String, String> task = companion.consumeStrings().fromTopics("dead-letter-topic-signals"))
    {
      return await().atMost(Duration.ofSeconds(30)).until(
        () -> task.getRecords().stream().map(record -> record.value()).filter(value -> value.contains(symbol))
          .findFirst().orElse(""),
        value -> !value.isEmpty());
    }
  }

  private List<ServeEvent> awaitAlerts(String symbol, int count)
  {
    return await().atMost(Duration.ofSeconds(30)).until(() -> alerts(symbol), events -> events.size() >= count);
  }

  private static RequestPatternBuilder postedFor(String symbol)
  {
    return postRequestedFor(anyUrl()).withRequestBody(matchingJsonPath("$.symbol", equalTo(symbol)));
  }

  private void publish(String symbol, Direction direction)
  {
    publishRaw(signal(symbol, direction.name()));
  }

  private void publishRaw(String value)
  {
    companion.produceStrings()
      .fromRecords(new ProducerRecord<>("signals", "test", value))
      .awaitCompletion();
  }
}
