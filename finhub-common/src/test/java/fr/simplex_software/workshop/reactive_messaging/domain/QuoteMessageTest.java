package fr.simplex_software.workshop.reactive_messaging.domain;

import org.junit.jupiter.api.*;

import java.math.*;

import static org.assertj.core.api.Assertions.*;

class QuoteMessageTest
{
  private static final Quote QUOTE =
    new Quote("AAPL", new BigDecimal("261.74"), new BigDecimal("259.71"));

  @Test
  void getPayload_quoteGiven_returnsSameQuote()
  {
    assertThat(new QuoteMessage(QUOTE).getPayload()).isSameAs(QUOTE);
  }

  @Test
  void ack_defaultCallback_completesWithoutRecursion()
  {
    assertThat(new QuoteMessage(QUOTE).ack().toCompletableFuture()).isCompleted();
  }

  @Test
  void nack_reasonGiven_completesWithoutRecursion()
  {
    assertThat(new QuoteMessage(QUOTE)
      .nack(new IllegalStateException("downstream refused the quote"))
      .toCompletableFuture()).isCompleted();
  }

  @Test
  void nack_nullReason_throwsIllegalArgument()
  {
    assertThatThrownBy(() -> new QuoteMessage(QUOTE).nack(null))
      .isInstanceOf(IllegalArgumentException.class);
  }
}
