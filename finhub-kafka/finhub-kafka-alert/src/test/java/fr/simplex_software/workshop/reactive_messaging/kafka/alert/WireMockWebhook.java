package fr.simplex_software.workshop.reactive_messaging.kafka.alert;

import com.github.tomakehurst.wiremock.*;
import com.github.tomakehurst.wiremock.core.*;
import io.quarkus.test.common.*;

import java.util.*;

public class WireMockWebhook implements QuarkusTestResourceLifecycleManager
{
  static WireMockServer SERVER;

  @Override
  public Map<String, String> start()
  {
    SERVER = new WireMockServer(WireMockConfiguration.options().dynamicPort()
      .asynchronousResponseEnabled(true).asynchronousResponseThreads(8).containerThreads(64).jettyAcceptQueueSize(4096));
    SERVER.start();
    return Map.of("quarkus.rest-client.alert.url", SERVER.baseUrl());
  }

  @Override
  public void stop()
  {
    SERVER.stop();
  }
}
