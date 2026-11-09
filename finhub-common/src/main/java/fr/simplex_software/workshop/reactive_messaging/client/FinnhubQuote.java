package fr.simplex_software.workshop.reactive_messaging.client;

import jakarta.json.bind.annotation.JsonbProperty;

import java.math.BigDecimal;
import java.util.Objects;

public record FinnhubQuote(@JsonbProperty("c") BigDecimal current,
                           @JsonbProperty("pc") BigDecimal previousClose)
{
  public FinnhubQuote
  {
    current = Objects.requireNonNullElse(current, BigDecimal.ZERO);
    previousClose = Objects.requireNonNullElse(previousClose, BigDecimal.ZERO);
  }
}
