package fr.simplex_software.workshop.reactive_messaging.kafka.processor;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import fr.simplex_software.workshop.reactive_messaging.signal.*;
import jakarta.enterprise.context.*;
import org.eclipse.microprofile.reactive.messaging.*;

@ApplicationScoped
public class QuoteProcessor
{
  private final PriceMovementEvaluator evaluator;

  public QuoteProcessor(PriceMovementEvaluator evaluator)
  {
    this.evaluator = evaluator;
  }

  @Incoming("quotes")
  @Outgoing("signals")
  public PriceSignal evaluate(Quote quote)
  {
    return evaluator.evaluate(quote);
  }
}
