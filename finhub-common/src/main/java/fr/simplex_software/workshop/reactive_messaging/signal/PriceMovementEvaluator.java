package fr.simplex_software.workshop.reactive_messaging.signal;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import jakarta.enterprise.context.*;

import java.math.*;
import java.util.*;
import java.util.concurrent.*;

@ApplicationScoped
public class PriceMovementEvaluator
{
  private final Map<String, BigDecimal> lastPrices = new ConcurrentHashMap<>();

  public PriceSignal evaluate(Quote quote)
  {
    BigDecimal lastPrice = lastPrices.put(quote.symbol(), quote.price());
    BigDecimal reference = lastPrice != null ? lastPrice : quote.previousClose();
    BigDecimal change = quote.price().subtract(reference);
    BigDecimal changePercent = reference.signum() != 0
      ? change.movePointRight(2).divide(reference, 2, RoundingMode.HALF_UP)
      : BigDecimal.ZERO;
    return new PriceSignal(quote.symbol(), quote.price(), direction(change), change, changePercent);
  }

  private static Direction direction(BigDecimal change)
  {
    int sign = change.signum();
    if (sign > 0) return Direction.UP;
    if (sign < 0) return Direction.DOWN;
    return Direction.FLAT;
  }
}
