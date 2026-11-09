package fr.simplex_software.workshop.reactive_messaging.domain;

import java.math.BigDecimal;

public record PriceSignal(String symbol, BigDecimal price, Direction direction,
  BigDecimal change, BigDecimal changePercent)
{
}
