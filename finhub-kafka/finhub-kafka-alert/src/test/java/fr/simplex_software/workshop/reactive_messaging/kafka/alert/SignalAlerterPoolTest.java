package fr.simplex_software.workshop.reactive_messaging.kafka.alert;

import io.quarkus.test.common.*;
import io.quarkus.test.junit.*;
import io.quarkus.test.kafka.*;
import io.smallrye.reactive.messaging.kafka.companion.*;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;

import static fr.simplex_software.workshop.reactive_messaging.kafka.alert.AlertBurst.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

@QuarkusTest
@TestProfile(SignalAlerterPoolTest.SmallPool.class)
@WithTestResource(KafkaCompanionResource.class)
@WithTestResource(WireMockWebhook.class)
class SignalAlerterPoolTest
{
  private static final int LIMIT = 32;
  private static final int POOL = 8;

  public static class SmallPool implements QuarkusTestProfile
  {
    @Override
    public Map<String, String> getConfigOverrides()
    {
      return Map.of("smallrye.messaging.worker.alerts.max-concurrency", String.valueOf(LIMIT),
        "quarkus.rest-client.alert.connection-pool-size", String.valueOf(POOL));
    }
  }

  @InjectKafkaCompanion
  KafkaCompanion companion;

  @BeforeEach
  void reset()
  {
    resetWebhook();
  }

  @Test
  void alert_poolBelowLimit_capsConcurrencyWithoutFailure()
  {
    publishDelayed(companion, "POOLED", 64, 500);
    List<Long> starts = awaitStarts("POOLED", 64, 500, POOL);
    assertThat(peakOverlap(starts, 500)).isEqualTo(POOL);
    awaitCompletion(starts, 500);
  }

  @Test
  void alert_connectionWaitExceedsTimeout_routesToDeadLetterTopic()
  {
    try (ConsumerTask<String, String> deadLetters = companion.consumeStrings().fromTopics("dead-letter-topic-signals"))
    {
      publishDelayed(companion, "STARVED", LIMIT, 2000);
      await().atMost(Duration.ofSeconds(30)).until(() -> alerts("STARVED").size() + starved(deadLetters) == LIMIT);
      assertThat(starved(deadLetters)).isPositive();
    }
    awaitCompletion(alerts("STARVED").stream().map(event -> event.getRequest().getLoggedDate().getTime()).sorted().toList(), 2000);
  }

  private static long starved(ConsumerTask<String, String> deadLetters)
  {
    return deadLetters.getRecords().stream().filter(record -> record.value().contains("STARVED")).count();
  }
}
