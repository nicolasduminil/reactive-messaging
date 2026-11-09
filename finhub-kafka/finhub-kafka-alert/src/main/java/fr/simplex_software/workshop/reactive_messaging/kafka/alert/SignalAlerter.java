package fr.simplex_software.workshop.reactive_messaging.kafka.alert;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.smallrye.common.annotation.*;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.*;
import org.eclipse.microprofile.reactive.messaging.*;
import org.eclipse.microprofile.rest.client.inject.*;

@ApplicationScoped
public class SignalAlerter
{
  private final AlertClient client;

  public SignalAlerter(@RestClient AlertClient client)
  {
    this.client = client;
  }

  @Incoming("signals")
  @RunOnVirtualThread
  @Blocking("alerts")
  public void alert(PriceSignal signal)
  {
    if (signal.direction() != Direction.FLAT)
      client.send(signal);
  }
}
