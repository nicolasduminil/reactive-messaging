package fr.simplex_software.workshop.reactive_messaging.domain;

import java.math.BigDecimal;

public record Quote(String symbol, BigDecimal price, BigDecimal previousClose)
{
}
