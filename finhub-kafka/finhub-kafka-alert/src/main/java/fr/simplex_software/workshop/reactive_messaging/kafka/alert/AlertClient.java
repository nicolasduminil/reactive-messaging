package fr.simplex_software.workshop.reactive_messaging.kafka.alert;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import jakarta.ws.rs.*;
import org.eclipse.microprofile.rest.client.inject.*;

@RegisterRestClient(configKey = "alert")
public interface AlertClient
{
  @POST
  void send(PriceSignal signal);
}
