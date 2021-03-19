package helpers;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.concurrent.ConcurrentSkipListMap;

public class LocalMeter {

  private final long windowSeconds = 900;
  private final ConcurrentSkipListMap<Long, Double> values = new ConcurrentSkipListMap<>();

  private long now() {
    return System.currentTimeMillis()/1000;
  }

  public void add(Number value) {
    long now = now();
    values.headMap(now - windowSeconds).clear();
    values.compute(now, (k, v) -> {
      return (v == null ? 0.0 : v) + value.doubleValue();
    });
  }

  public Number avg(long windowSeconds) {
    return sum(windowSeconds).doubleValue() / windowSeconds;
  }

  public Number sum(long windowSeconds) {
    double sum = 0;
    long now = now();
    long fromKey = now - windowSeconds;
    long toKey = now;
    for (double value : values.subMap(fromKey, true, toKey, false).values())
      sum += value;
    return sum;
  }

  public String toString() {
    return String.format("%s/%s/%s %s/%s/%s",
      num(avg(1)), num(avg(5)), num(avg(15)),
      num(avg(60)), num(avg(300)), num(avg(900)));
  }

  private Number num(Number num) {
    return num.longValue();
    // DecimalFormat df = new DecimalFormat("#.#");
    // df.setRoundingMode(RoundingMode.HALF_EVEN);
    // return df.format(num);
  }

}
