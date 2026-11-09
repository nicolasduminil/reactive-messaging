package fr.simplex_software.workshop.reactive_messaging.feed;

import fr.simplex_software.workshop.reactive_messaging.client.*;
import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.smallrye.mutiny.*;
import org.junit.jupiter.api.*;

import java.math.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.*;

class QuoteSourceTest
{
  private static final FinnhubConfig CONFIG = new FinnhubConfig()
  {
    @Override
    public String symbol()
    {
      return "AAPL";
    }

    @Override
    public Optional<String> token()
    {
      return Optional.empty();
    }

    @Override
    public long intervalSeconds()
    {
      return 1;
    }
  };

  @Test
  void stream_clientFails_skipsFailureAndContinues()
  {
    AtomicInteger calls = new AtomicInteger();
    FinnhubClient client = (symbol, token) -> Uni.createFrom().item(() ->
    {
      if (calls.getAndIncrement() == 0)
        throw new IllegalStateException("Finnhub is unavailable");
      return new FinnhubQuote(new BigDecimal("261.74"), new BigDecimal("259.71"));
    });

    Quote quote = new QuoteSource(client, CONFIG).stream()
      .select().first()
      .toUni()
      .await().atMost(Duration.ofSeconds(10));

    assertThat(calls).hasValueGreaterThanOrEqualTo(2);
    assertThat(quote.symbol()).isEqualTo("AAPL");
    assertThat(quote.price()).isEqualByComparingTo("261.74");
    assertThat(quote.previousClose()).isEqualByComparingTo("259.71");
  }
}
