package app;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AtomicDouble;

public class LocalMeter {

  private final int windowSeconds = 900;
  private final AtomicDouble total = new AtomicDouble();
  private final ConcurrentSkipListMap<Long, Double> values = new ConcurrentSkipListMap<>();

  public long now() {
    return System.currentTimeMillis();
  }

  public void mark(Number value) {
    final long now = now();
    total.addAndGet(value.doubleValue());
    values.headMap(now - windowSeconds * 1000).clear();
    values.compute(now, (k, v) -> {
      return (v == null ? 0.0 : v) + value.doubleValue();
    });
  }

  public Number avg(long windowSeconds) {
    return sum(windowSeconds).doubleValue() / windowSeconds;
  }

  public Number sum(long windowSeconds) {
    final long now = now();
    // values.headMap(now - windowSeconds * 1000).clear();
    long fromKey = now - windowSeconds * 1000;
    long toKey = now;
    long sum = 0;
    for (double value : values.subMap(fromKey, true, toKey, false).values())
      sum += value;
    return sum;
  }

  public Number total() {
    return total.get();
  }

  public String toString() {
    // return String.format("%s(%s/s)", total, avg(15).longValue());
    return String.format("%s %s/%s/%s %s/%s/%s", total.longValue(),
      avg(1).longValue(), avg(5).longValue(), avg(15).longValue(),
      avg(60).longValue(), avg(300).longValue(), avg(900).longValue());
  }

}
