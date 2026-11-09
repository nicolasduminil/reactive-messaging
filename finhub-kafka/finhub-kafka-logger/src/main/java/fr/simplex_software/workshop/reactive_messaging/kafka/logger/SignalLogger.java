package fr.simplex_software.workshop.reactive_messaging.kafka.logger;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import fr.simplex_software.workshop.reactive_messaging.signal.*;
import io.smallrye.mutiny.*;
import io.smallrye.mutiny.operators.multi.processors.*;
import jakarta.enterprise.context.*;
import org.eclipse.microprofile.reactive.messaging.*;

@ApplicationScoped
public class SignalLogger
{
  private final PriceSignalLogger logger;
  private final BroadcastProcessor<PriceSignal> broadcast = BroadcastProcessor.create();

  public SignalLogger(PriceSignalLogger logger)
  {
    this.logger = logger;
  }

  @Incoming("signals")
  public void onSignal(PriceSignal signal)
  {
    logger.log(signal);
    broadcast.onNext(signal);
  }

  public Multi<PriceSignal> signals()
  {
    return broadcast.onOverflow().drop();
  }
}
