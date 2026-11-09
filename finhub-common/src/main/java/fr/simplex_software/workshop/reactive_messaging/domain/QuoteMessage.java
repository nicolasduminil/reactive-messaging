package fr.simplex_software.workshop.reactive_messaging.domain;

import io.quarkus.logging.*;
import org.eclipse.microprofile.reactive.messaging.*;

import java.util.concurrent.*;
import java.util.function.*;

public class QuoteMessage implements Message<Quote>
{
  private static final CompletionStage<Void> SETTLED = CompletableFuture.completedFuture(null);

  private final Quote payload;

  public QuoteMessage(Quote payload)
  {
    this.payload = payload;
  }

  @Override
  public Quote getPayload()
  {
    return payload;
  }

  @Override
  public Supplier<CompletionStage<Void>> getAck()
  {
    return () ->
    {
      Log.infof("Acknowledgment for %s", payload);
      return SETTLED;
    };
  }

  @Override
  public Function<Throwable, CompletionStage<Void>> getNack()
  {
    return reason ->
    {
      Log.infof("Negative acknowledgment for %s, the reason is %s", payload, reason);
      return SETTLED;
    };
  }
}
