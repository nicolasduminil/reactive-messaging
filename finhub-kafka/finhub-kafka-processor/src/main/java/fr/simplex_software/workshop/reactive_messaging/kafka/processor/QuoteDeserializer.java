package fr.simplex_software.workshop.reactive_messaging.kafka.processor;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.quarkus.kafka.client.serialization.*;

public class QuoteDeserializer extends JsonbDeserializer<Quote>
{
  public QuoteDeserializer()
  {
    super(Quote.class);
  }
}
