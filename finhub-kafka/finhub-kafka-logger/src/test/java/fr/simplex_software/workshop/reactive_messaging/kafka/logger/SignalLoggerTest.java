package fr.simplex_software.workshop.reactive_messaging.kafka.logger;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import fr.simplex_software.workshop.reactive_messaging.signal.*;
import io.quarkus.test.common.*;
import io.quarkus.test.junit.*;
import io.quarkus.test.kafka.*;
import io.smallrye.reactive.messaging.kafka.companion.*;
import jakarta.inject.*;
import org.apache.kafka.clients.producer.*;
import org.junit.jupiter.api.*;

import java.time.*;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

@QuarkusTest
@WithTestResource(KafkaCompanionResource.class)
class SignalLoggerTest
{
  @InjectKafkaCompanion
  KafkaCompanion companion;

  @Inject
  PriceSignalLogger logger;

  @Test
  void onSignal_validSignals_logsLatest()
  {
    publish("AAPL", Direction.UP, "261.74", "2.03");
    awaitSignal("AAPL", Direction.UP, "261.74");

    publish("AAPL", Direction.DOWN, "260.00", "-1.74");
    awaitSignal("AAPL", Direction.DOWN, "260.00");
  }

  @Test
  void onSignal_malformedRecord_skipsItAndLogsNext()
  {
    publishRaw("this is not JSON");
    publish("MSFT", Direction.DOWN, "400.00", "-10.00");

    awaitSignal("MSFT", Direction.DOWN, "400.00");
  }

  private void publish(String symbol, Direction direction, String price, String change)
  {
    publishRaw("""
      {"symbol":"%s","price":%s,"direction":"%s","change":%s,"changePercent":0.78}"""
      .formatted(symbol, price, direction, change));
  }

  private void publishRaw(String value)
  {
    companion.produceStrings()
      .fromRecords(new ProducerRecord<>("signals", "test", value))
      .awaitCompletion();
  }

  private void awaitSignal(String symbol, Direction direction, String price)
  {
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
    {
      PriceSignal signal = logger.latest();
      assertThat(signal).isNotNull();
      assertThat(signal.symbol()).isEqualTo(symbol);
      assertThat(signal.direction()).isEqualTo(direction);
      assertThat(signal.price()).isEqualByComparingTo(price);
    });
  }
}
