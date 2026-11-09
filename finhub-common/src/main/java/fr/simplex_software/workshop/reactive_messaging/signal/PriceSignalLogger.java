package fr.simplex_software.workshop.reactive_messaging.signal;

import fr.simplex_software.workshop.reactive_messaging.domain.*;
import io.quarkus.logging.*;
import jakarta.enterprise.context.*;

import java.util.concurrent.atomic.*;

@ApplicationScoped
public class PriceSignalLogger
{
  private final AtomicReference<PriceSignal> latest = new AtomicReference<>();

  public void log(PriceSignal signal)
  {
    String line = format(signal);
    latest.set(signal);
    Log.info(line);
  }

  public PriceSignal latest()
  {
    return latest.get();
  }

  private static String format(PriceSignal s)
  {
    String arrow = switch (s.direction())
    {
      case UP -> "▲";
      case DOWN -> "▼";
      case FLAT -> "▬";
    };
    return "%s %.2f USD %s %+.2f (%+.2f%%)".formatted(s.symbol(), s.price(),
      arrow, s.change(), s.changePercent());
  }
}
