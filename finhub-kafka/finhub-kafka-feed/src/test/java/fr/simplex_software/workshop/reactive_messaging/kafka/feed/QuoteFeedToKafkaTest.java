package fr.simplex_software.workshop.reactive_messaging.kafka.feed;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.quarkus.test.common.*;
import io.quarkus.test.junit.*;
import io.quarkus.test.kafka.*;
import io.smallrye.reactive.messaging.kafka.companion.*;
import jakarta.inject.*;
import jakarta.json.bind.*;
import org.eclipse.microprofile.reactive.messaging.*;
import org.junit.jupiter.api.*;

import java.math.*;
import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

@QuarkusTest
@WithTestResource(KafkaCompanionResource.class)
class QuoteFeedToKafkaTest
{
  private static final Jsonb JSONB = JsonbBuilder.create();

  @InjectKafkaCompanion
  KafkaCompanion companion;

  @Inject
  @Channel("quotes")
  Emitter<Quote> quotes;

  @Test
  void quotes_quotesEmitted_serialisesThemOntoTheTopic()
  {
    quotes.send(new Quote("AAPL", new BigDecimal("261.74"), new BigDecimal("259.71")));
    quotes.send(new Quote("AAPL", new BigDecimal("260.00"), new BigDecimal("259.71")));

    List<Quote> published = companion.consumeStrings()
      .fromTopics("quotes", 2, Duration.ofSeconds(30))
      .awaitCompletion()
      .getRecords().stream()
      .map(record -> JSONB.fromJson(record.value(), Quote.class))
      .toList();

    assertThat(published.get(0).symbol()).isEqualTo("AAPL");
    assertThat(published.get(0).price()).isEqualByComparingTo("261.74");
    assertThat(published.get(0).previousClose()).isEqualByComparingTo("259.71");
    assertThat(published.get(1).price()).isEqualByComparingTo("260.00");
  }
}
