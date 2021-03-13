package main;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.concurrent.ConcurrentSkipListMap;

public class LocalMeter {

  private final int windowSeconds = 900;
  private final ConcurrentSkipListMap<Long, Double> values = new ConcurrentSkipListMap<>();

  private long now() {
    return System.currentTimeMillis();
  }

  public void mark(Number value) {
    mark(value, now());
  }
  public void mark(Number value, long now) {
    values.headMap(now - windowSeconds * 1000).clear();
    values.compute(now, (k, v) -> {
      return (v == null ? 0.0 : v) + value.doubleValue();
    });
  }

  public Number avg(long windowSeconds) {
    return avg(windowSeconds, now());
  }
  public Number avg(long windowSeconds, long now) {
    return sum(windowSeconds, now).doubleValue() / windowSeconds;
  }

  public Number sum(long windowSeconds) {
    return sum(windowSeconds, now());
  }
  public Number sum(long windowSeconds, long now) {
    // values.headMap(now - windowSeconds * 1000).clear();
    long fromKey = now - windowSeconds * 1000;
    long toKey = now;
    long sum = 0;
    for (double value : values.subMap(fromKey, true, toKey, false).values())
      sum += value;
    return sum;
  }

  public String toString() {
    return String.format("%s/%s/%s %s/%s/%s",
      num(avg(1)), num(avg(5)), num(avg(15)),
      num(avg(60)), num(avg(300)), num(avg(900)));
  }

  private String num(Number src) {
    DecimalFormat df = new DecimalFormat("#.#");
    df.setRoundingMode(RoundingMode.HALF_EVEN);
    return df.format(src);
  }

}
