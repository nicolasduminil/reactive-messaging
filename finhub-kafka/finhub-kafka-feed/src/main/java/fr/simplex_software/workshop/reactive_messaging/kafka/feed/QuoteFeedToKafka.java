package fr.simplex_software.workshop.reactive_messaging.kafka.feed;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import fr.simplex_software.workshop.reactive_messaging.feed.*;
import io.quarkus.arc.profile.*;
import io.smallrye.mutiny.*;
import jakarta.enterprise.context.*;
import org.eclipse.microprofile.reactive.messaging.*;

@ApplicationScoped
@UnlessBuildProfile("test")
public class QuoteFeedToKafka
{
  private final QuoteSource source;

  public QuoteFeedToKafka(QuoteSource source)
  {
    this.source = source;
  }

  @Outgoing("quotes")
  public Multi<Message<Quote>> quotes()
  {
    return source.stream().map(QuoteMessage::new);
  }
}
