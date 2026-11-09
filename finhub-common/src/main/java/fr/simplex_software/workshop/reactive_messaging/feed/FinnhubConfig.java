package fr.simplex_software.workshop.reactive_messaging.feed;

import io.smallrye.config.*;

import java.util.*;

@ConfigMapping(prefix = "finnhub")
public interface FinnhubConfig
{
  @WithDefault("AAPL")
  String symbol();

  Optional<String> token();

  @WithDefault("15")
  long intervalSeconds();
}
