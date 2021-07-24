package helpers;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

public class AbstractThrottleGuava implements AbstractThrottle {

  private final Supplier<Number> rateProvider;
  private final RateLimiter limiter = RateLimiter.create(5.0);
  private final Timer timer = new HashedWheelTimer();

  public AbstractThrottleGuava(Supplier<Number> rateProvider) {
    this.rateProvider = rateProvider;
  }

  public ListenableFuture<?> asyncAcquire(Number permits) {
    limiter.setRate(rateProvider.get().doubleValue());
    return new AbstractFuture<Void>() {
      {
        doAcquire(permits);
      }
      void doAcquire(Number permits) {
        if (limiter.tryAcquire(permits.intValue()))
          set(null); // resolve future
        else {
          double delayMs = 1000 * permits.doubleValue() / limiter.getRate();
          // random backoff
          // double delayMs =
          // 1000*newRandom().nextDouble()*permits.doubleValue()/limiter.getRate();
          // System.out.println("delay:"+delay);
          timer.newTimeout(timeout -> {
            doAcquire(permits);
          }, Double.valueOf(delayMs).longValue(), TimeUnit.MILLISECONDS);
        }
      }
    };
  }

}
