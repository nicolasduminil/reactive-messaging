package fr.simplex_software.workshop.reactive_messaging.kafka.processor;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.quarkus.test.common.*;
import io.quarkus.test.junit.*;
import io.quarkus.test.kafka.*;
import io.smallrye.reactive.messaging.kafka.companion.*;
import jakarta.json.bind.*;
import org.apache.kafka.clients.producer.*;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

@QuarkusTest
@WithTestResource(KafkaCompanionResource.class)
class QuoteProcessorTest
{
  private static final Jsonb JSONB = JsonbBuilder.create();

  @InjectKafkaCompanion
  KafkaCompanion companion;

  @Test
  void evaluate_validQuotes_publishesSignals()
  {
    publish("AAPL", "261.74", "259.71");
    publish("AAPL", "260.00", "259.71");

    List<PriceSignal> signals = awaitSignals("AAPL", 2);

    assertThat(signals.get(0).direction()).isEqualTo(Direction.UP);
    assertThat(signals.get(0).price()).isEqualByComparingTo("261.74");
    assertThat(signals.get(1).direction()).isEqualTo(Direction.DOWN);
    assertThat(signals.get(1).price()).isEqualByComparingTo("260.00");
  }

  @Test
  void evaluate_malformedRecord_skipsItAndProcessesNext()
  {
    publishRaw("this is not JSON");
    publish("MSFT", "400.00", "410.00");

    List<PriceSignal> signals = awaitSignals("MSFT", 1);

    assertThat(signals.get(0).direction()).isEqualTo(Direction.DOWN);
    assertThat(signals.get(0).price()).isEqualByComparingTo("400.00");
  }

  private List<PriceSignal> awaitSignals(String symbol, int count)
  {
    try (ConsumerTask<String, String> task = companion.consumeStrings().fromTopics("signals"))
    {
      return await().atMost(Duration.ofSeconds(30)).until(
        () -> task.getRecords().stream()
          .map(record -> JSONB.fromJson(record.value(), PriceSignal.class))
          .filter(signal -> symbol.equals(signal.symbol()))
          .toList(),
        signals -> signals.size() >= count);
    }
  }

  private void publish(String symbol, String price, String previousClose)
  {
    publishRaw("""
      {"symbol":"%s","price":%s,"previousClose":%s}""".formatted(symbol, price, previousClose));
  }

  private void publishRaw(String value)
  {
    companion.produceStrings()
      .fromRecords(new ProducerRecord<>("quotes", "test", value))
      .awaitCompletion();
  }
}
