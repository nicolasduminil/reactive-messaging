package fr.simplex_software.workshop.reactive_messaging.feed;

import fr.simplex_software.workshop.reactive_messaging.client.*;
import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.quarkus.arc.profile.*;
import io.quarkus.logging.*;
import io.smallrye.mutiny.*;
import jakarta.enterprise.context.*;
import org.eclipse.microprofile.rest.client.inject.*;

import java.time.*;

@ApplicationScoped
@UnlessBuildProfile("test")
public class QuoteSource
{
  private final FinnhubClient finnhub;
  private final FinnhubConfig config;

  public QuoteSource(@RestClient FinnhubClient finnhub, FinnhubConfig config)
  {
    this.finnhub = finnhub;
    this.config = config;
  }

  public Multi<Quote> stream()
  {
    String symbol = config.symbol();
    return finnhub.quote(symbol, config.token().orElse(""))
      .map(quote -> new Quote(symbol, quote.current(), quote.previousClose()))
      .onFailure().invoke(err -> Log.warnf("Finnhub fetch failed for %s: %s", symbol, err.getMessage()))
      .onFailure().recoverWithNull()
      .repeat().withDelay(Duration.ofSeconds(config.intervalSeconds())).indefinitely();
  }
}
