package fr.simplex_software.workshop.reactive_messaging.pipeline;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import fr.simplex_software.workshop.reactive_messaging.signal.*;
import jakarta.enterprise.context.*;
import org.eclipse.microprofile.reactive.messaging.*;

@ApplicationScoped
public class QuotePipeline
{
  private final PriceMovementEvaluator evaluator;
  private final PriceSignalLogger logger;

  public QuotePipeline(PriceMovementEvaluator evaluator, PriceSignalLogger logger)
  {
    this.evaluator = evaluator;
    this.logger = logger;
  }

  @Incoming("quotes")
  @Outgoing("signals")
  public PriceSignal evaluate(Quote quote)
  {
    return evaluator.evaluate(quote);
  }

  @Incoming("signals")
  public void onSignal(PriceSignal signal)
  {
    logger.log(signal);
  }
}
