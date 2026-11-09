package fr.simplex_software.workshop.reactive_messaging.kafka.alert;

import com.github.tomakehurst.wiremock.stubbing.*;
import io.smallrye.reactive.messaging.kafka.companion.*;
import org.apache.kafka.clients.producer.*;

import java.time.*;
import java.util.*;
import java.util.stream.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static fr.simplex_software.workshop.reactive_messaging.kafka.alert.WireMockWebhook.*;
import static org.awaitility.Awaitility.*;

final class AlertBurst
{
  private AlertBurst()
  {
  }

  static void resetWebhook()
  {
    SERVER.resetAll();
    SERVER.stubFor(post(anyUrl()).willReturn(aResponse().withStatus(204)));
  }

  static String signal(String symbol, String direction)
  {
    return """
      {"symbol":"%s","price":261.74,"direction":"%s","change":2.03,"changePercent":0.78}"""
      .formatted(symbol, direction);
  }

  static void publishDelayed(KafkaCompanion companion, String symbol, int count, int delayMs)
  {
    SERVER.stubFor(post(anyUrl()).withRequestBody(matchingJsonPath("$.symbol", equalTo(symbol)))
      .willReturn(aResponse().withStatus(204).withFixedDelay(delayMs)));
    companion.produceStrings()
      .fromRecords(IntStream.range(0, count)
        .mapToObj(i -> new ProducerRecord<>("signals", "test", signal(symbol, "UP"))).toList())
      .awaitCompletion(Duration.ofSeconds(60));
  }

  static List<Long> awaitStarts(String symbol, int count, int delayMs, int concurrency)
  {
    Duration expected = Duration.ofMillis((long) Math.ceil((double) count / concurrency) * delayMs);
    return await().atMost(Duration.ofSeconds(60).plus(expected)).pollInterval(Duration.ofMillis(200))
      .until(() -> alerts(symbol), events -> events.size() >= count).stream()
      .map(event -> event.getRequest().getLoggedDate().getTime()).sorted().toList();
  }

  static int peakOverlap(List<Long> starts, int delayMs)
  {
    int peak = 0;
    for (int first = 0, last = 0; last < starts.size(); last++)
    {
      while (starts.get(first) + delayMs <= starts.get(last))
        first++;
      peak = Math.max(peak, last - first + 1);
    }
    return peak;
  }

  static void awaitCompletion(List<Long> starts, int delayMs)
  {
    long end = starts.getLast() + delayMs;
    await().atMost(Duration.ofMillis(delayMs).plusSeconds(5)).until(() -> System.currentTimeMillis() > end);
  }

  static List<ServeEvent> alerts(String symbol)
  {
    return SERVER.getAllServeEvents().stream()
      .filter(event -> event.getRequest().getBodyAsString().contains("\"symbol\":\"%s\"".formatted(symbol)))
      .toList();
  }
}
