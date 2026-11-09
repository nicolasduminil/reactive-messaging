package fr.simplex_software.workshop.reactive_messaging.client;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@RegisterRestClient(configKey = "finnhub")
public interface FinnhubClient
{
  @GET
  @Path("/quote")
  Uni<FinnhubQuote> quote(@QueryParam("symbol") String symbol, @QueryParam("token") String token);
}
