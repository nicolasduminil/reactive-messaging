package fr.simplex_software.workshop.reactive_messaging.kafka.logger;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.smallrye.mutiny.*;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import org.jboss.resteasy.reactive.*;

@Path("/api/v1/signals")
public class SignalStreamResource
{
  private final SignalLogger signalLogger;

  public SignalStreamResource(SignalLogger signalLogger)
  {
    this.signalLogger = signalLogger;
  }

  @GET
  @Path("/stream")
  @Produces(MediaType.SERVER_SENT_EVENTS)
  @RestStreamElementType(MediaType.APPLICATION_JSON)
  public Multi<PriceSignal> stream()
  {
    return signalLogger.signals();
  }
}
