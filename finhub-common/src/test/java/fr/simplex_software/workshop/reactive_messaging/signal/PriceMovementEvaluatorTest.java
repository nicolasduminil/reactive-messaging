package fr.simplex_software.workshop.reactive_messaging.signal;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import org.junit.jupiter.api.*;

import java.math.*;

import static org.assertj.core.api.Assertions.*;

class PriceMovementEvaluatorTest
{
  private final PriceMovementEvaluator evaluator = new PriceMovementEvaluator();

  private static Quote quote(String symbol, String price, String previousClose)
  {
    return new Quote(symbol, new BigDecimal(price), new BigDecimal(previousClose));
  }

  @Test
  void evaluate_firstQuote_comparesWithPreviousClose()
  {
    PriceSignal signal = evaluator.evaluate(quote("AAPL", "261.74", "259.71"));
    assertThat(signal.direction()).isEqualTo(Direction.UP);
    assertThat(signal.change()).isEqualByComparingTo("2.03");
  }

  @Test
  void evaluate_successiveQuotes_followsPriceDirection()
  {
    assertThat(evaluator.evaluate(quote("AAPL", "100.00", "100.00")).direction()).isEqualTo(Direction.FLAT);
    assertThat(evaluator.evaluate(quote("AAPL", "101.00", "100.00")).direction()).isEqualTo(Direction.UP);
    assertThat(evaluator.evaluate(quote("AAPL", "100.50", "100.00")).direction()).isEqualTo(Direction.DOWN);
  }

  @Test
  void evaluate_unchangedPrice_returnsFlat()
  {
    evaluator.evaluate(quote("AAPL", "100.00", "90.00"));
    PriceSignal signal = evaluator.evaluate(quote("AAPL", "100.00", "90.00"));
    assertThat(signal.direction()).isEqualTo(Direction.FLAT);
    assertThat(signal.change()).isEqualByComparingTo(BigDecimal.ZERO);
  }

  @Test
  void evaluate_zeroReference_returnsZeroPercent()
  {
    PriceSignal signal = evaluator.evaluate(quote("AAPL", "10.00", "0"));
    assertThat(signal.direction()).isEqualTo(Direction.UP);
    assertThat(signal.changePercent()).isEqualByComparingTo(BigDecimal.ZERO);
  }

  @Test
  void evaluate_differentSymbols_comparesEachWithOwnHistory()
  {
    evaluator.evaluate(quote("AAPL", "261.74", "259.71"));
    PriceSignal signal = evaluator.evaluate(quote("MSFT", "400.00", "410.00"));
    assertThat(signal.direction()).isEqualTo(Direction.DOWN);
    assertThat(signal.change()).isEqualByComparingTo("-10.00");
  }
}
