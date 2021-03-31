package helpers;

import java.util.function.Supplier;

import com.google.common.util.concurrent.RateLimiter;

public class AbstractThrottleGuava implements AbstractThrottle {

  private final Supplier<Number> rateProvider;
  private final RateLimiter limiter = RateLimiter.create(5.0);

  public AbstractThrottleGuava(Supplier<Number> rateProvider) {
    this.rateProvider = rateProvider;
  }

  public void acquire(Number permits) {
    limiter.setRate(rateProvider.get().doubleValue());
    if (permits.intValue() > 0)
      limiter.acquire(permits.intValue());
  }

}
