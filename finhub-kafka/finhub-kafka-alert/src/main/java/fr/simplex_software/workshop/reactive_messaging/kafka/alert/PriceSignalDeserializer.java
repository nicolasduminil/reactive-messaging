package fr.simplex_software.workshop.reactive_messaging.kafka.alert;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.quarkus.kafka.client.serialization.*;

public class PriceSignalDeserializer extends JsonbDeserializer<PriceSignal>
{
  public PriceSignalDeserializer()
  {
    super(PriceSignal.class);
  }
}
