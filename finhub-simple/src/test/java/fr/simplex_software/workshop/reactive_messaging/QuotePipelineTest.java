package fr.simplex_software.workshop.reactive_messaging;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import fr.simplex_software.workshop.reactive_messaging.signal.*;
import io.quarkus.test.junit.*;
import io.smallrye.reactive.messaging.memory.*;
import jakarta.enterprise.inject.*;
import jakarta.inject.*;
import org.eclipse.microprofile.reactive.messaging.*;
import org.junit.jupiter.api.*;

import java.math.*;
import java.time.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

@QuarkusTest
class QuotePipelineTest
{
  @Inject
  @Any
  InMemoryConnector connector;

  @Inject
  PriceSignalLogger logger;

  @Test
  void evaluate_validQuotes_acknowledgesAndLogsSignal()
  {
    InMemorySource<Message<Quote>> quotes = connector.source("quotes");

    CompletableFuture<Void> risingAck = new CompletableFuture<>();
    quotes.send(quoteMessage("261.74", "259.71", risingAck));
    await().atMost(Duration.ofSeconds(5)).until(risingAck::isDone);
    awaitSignal(Direction.UP, "261.74");

    CompletableFuture<Void> fallingAck = new CompletableFuture<>();
    quotes.send(quoteMessage("260.00", "259.71", fallingAck));
    await().atMost(Duration.ofSeconds(5)).until(fallingAck::isDone);
    awaitSignal(Direction.DOWN, "260.00");
  }

  @Test
  void evaluate_nullPrice_nacksMessage()
  {
    InMemorySource<Message<Quote>> quotes = connector.source("quotes");
    CompletableFuture<Throwable> nacked = new CompletableFuture<>();

    quotes.send(Message.of(new Quote("MSFT", null, new BigDecimal("410.00")),
      () -> CompletableFuture.completedFuture(null),
      reason ->
      {
        nacked.complete(reason);
        return CompletableFuture.completedFuture(null);
      }));

    assertThat(nacked).succeedsWithin(Duration.ofSeconds(5)).isInstanceOf(NullPointerException.class);
  }

  private void awaitSignal(Direction direction, String price)
  {
    await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
    {
      PriceSignal signal = logger.latest();
      assertThat(signal).isNotNull();
      assertThat(signal.symbol()).isEqualTo("AAPL");
      assertThat(signal.direction()).isEqualTo(direction);
      assertThat(signal.price()).isEqualByComparingTo(price);
    });
  }

  private static Message<Quote> quoteMessage(String price, String previousClose, CompletableFuture<Void> acknowledged)
  {
    return Message.of(new Quote("AAPL", new BigDecimal(price), new BigDecimal(previousClose)),
      () ->
      {
        acknowledged.complete(null);
        return CompletableFuture.completedFuture(null);
      });
  }
}
