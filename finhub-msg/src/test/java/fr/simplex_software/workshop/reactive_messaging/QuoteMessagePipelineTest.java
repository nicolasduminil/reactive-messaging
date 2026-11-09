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
import java.util.function.*;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

@QuarkusTest
class QuoteMessagePipelineTest
{
  @Inject
  @Any
  InMemoryConnector connector;

  @Inject
  PriceSignalLogger logger;

  @Test
  void evaluate_validMessages_acknowledgesAndLogsSignal()
  {
    InMemorySource<Message<Quote>> quotes = connector.source("quotes");

    RecordingQuoteMessage rising = message("261.74", "259.71");
    quotes.send(rising);
    await().atMost(Duration.ofSeconds(5)).until(rising.acknowledged::isDone);
    awaitSignal(Direction.UP, "261.74");

    RecordingQuoteMessage falling = message("260.00", "259.71");
    quotes.send(falling);
    await().atMost(Duration.ofSeconds(5)).until(falling.acknowledged::isDone);
    awaitSignal(Direction.DOWN, "260.00");
  }

  @Test
  void evaluate_nullPrice_nacksMessage()
  {
    InMemorySource<Message<Quote>> quotes = connector.source("quotes");
    RecordingQuoteMessage invalid =
      new RecordingQuoteMessage(new Quote("MSFT", null, new BigDecimal("410.00")));

    quotes.send(invalid);

    assertThat(invalid.nacked).succeedsWithin(Duration.ofSeconds(5)).isInstanceOf(NullPointerException.class);
    assertThat(invalid.acknowledged).isNotDone();
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

  private static RecordingQuoteMessage message(String price, String previousClose)
  {
    return new RecordingQuoteMessage(
      new Quote("AAPL", new BigDecimal(price), new BigDecimal(previousClose)));
  }

  private static class RecordingQuoteMessage extends QuoteMessage
  {
    private final CompletableFuture<Void> acknowledged = new CompletableFuture<>();
    private final CompletableFuture<Throwable> nacked = new CompletableFuture<>();

    private RecordingQuoteMessage(Quote quote)
    {
      super(quote);
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck()
    {
      Supplier<CompletionStage<Void>> delegate = super.getAck();
      return () -> delegate.get().thenRun(() -> acknowledged.complete(null));
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack()
    {
      Function<Throwable, CompletionStage<Void>> delegate = super.getNack();
      return reason -> delegate.apply(reason).thenRun(() -> nacked.complete(reason));
    }
  }
}
