package fr.simplex_software.workshop.reactive_messaging.client;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class FinnhubQuoteTest
{
  private final Jsonb jsonb = JsonbBuilder.create();

  @Test
  void fromJson_allFieldsPresent_mapsCurrentAndPreviousClose()
  {
    FinnhubQuote q = jsonb.fromJson(
      "{\"c\":261.74,\"d\":2.03,\"dp\":0.78,\"pc\":259.71,\"t\":1582641000}", FinnhubQuote.class);
    assertThat(q.current()).isEqualByComparingTo("261.74");
    assertThat(q.previousClose()).isEqualByComparingTo("259.71");
  }

  @Test
  void fromJson_missingFields_defaultsToZero()
  {
    FinnhubQuote q = jsonb.fromJson("{}", FinnhubQuote.class);
    assertThat(q.current()).isEqualByComparingTo(BigDecimal.ZERO);
    assertThat(q.previousClose()).isEqualByComparingTo(BigDecimal.ZERO);
  }
}
