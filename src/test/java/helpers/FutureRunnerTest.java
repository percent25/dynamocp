package helpers;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.google.common.util.concurrent.*;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

public class FutureRunnerTest {

  private final int desiredCount = 111;
  private final AtomicInteger reportedCount = new AtomicInteger();
  private final AtomicInteger landedCount = new AtomicInteger();

  @Test
  public void randomOneLandingTest() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      new FutureRunner() {
        int count;
        int landedCount;
        {
          run(() -> {
            for (int i = 0; i < 111; ++i) {
              ++count;
              System.out.println("count:" + count);
              // run(() -> {
              // return Futures.immediateVoidFuture();
              // });
              if (new Random().nextInt() < Integer.MAX_VALUE / 2) {
                run(() -> {
                  return Futures.immediateVoidFuture();
                });
              } else {
                run(() -> {
                  return Futures.submit(() -> {
                    Thread.sleep(5);
                    return Futures.immediateVoidFuture();
                  }, executor);
                });
              }
            }
            return Futures.immediateVoidFuture();
          });
        }
        @Override
        protected void onLanded() {
          ++landedCount;
          System.out.println("[onLanded] landedCount:" + landedCount);
          assertThat(landedCount).isEqualTo(1);
        }
      }.get().get();
    } finally {
      executor.shutdown();
    }
    System.out.println("done");
  }

  @Test
  public void randomManyLandingsTest() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      ListenableFuture<?> lf = new FutureRunner() {
        {
          for (int i = 0; i < desiredCount; ++i) {
            reportedCount.incrementAndGet();
            System.out.println("count:" + reportedCount);
            // run(() -> {
            // return Futures.immediateVoidFuture();
            // });
            if (new Random().nextInt() < Integer.MAX_VALUE / 2) {
              run(() -> {
                return Futures.immediateVoidFuture();
              });
            } else {
              run(() -> {
                return Futures.submit(() -> {
                  return Futures.immediateVoidFuture();
                }, executor);
              });
            }
          }
        }
        @Override
        protected void onLanded() {
          landedCount.incrementAndGet();
          System.out.println("[onLanded] landedCount:" + landedCount);
        }
      }.get();
      lf.addListener(()->{
        System.out.println("[listener] reportedCount:" + reportedCount);
        System.out.println("[listener] landedCount:" + landedCount);
      }, MoreExecutors.directExecutor());
      lf.get();
      assertThat(reportedCount.get()).isBetween(0, desiredCount);
    } finally {
      executor.shutdown();
    }
    System.out.println("done");
  }

  @Test
  public void manyLandingsTest() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      ListenableFuture<?> lf = new FutureRunner() {
        {
          for (int i = 0; i < desiredCount; ++i) {
            reportedCount.incrementAndGet();
            System.out.println("count:" + reportedCount);
            run(() -> {
              return Futures.immediateVoidFuture();
            });
          }
        }
        @Override
        protected void onLanded() {
          landedCount.incrementAndGet();
          System.out.println("[onLanded] landedCount:" + landedCount);
        }
      }.get();
      lf.get();
      assertThat(reportedCount.get()).isEqualTo(desiredCount);
      assertThat(landedCount.get()).isEqualTo(desiredCount);
    // Thread.sleep(3000);
    } finally {
      executor.shutdown();
    }
    System.out.println("done");
  }

  @Test
  public void stressTest() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      ListenableFuture<?> lf = new FutureRunner() {
        {
          run(() -> {
            for (int i = 0; i < desiredCount; ++i) {
              if (new Random().nextInt() < Integer.MAX_VALUE / 2) {
                run(() -> {
                  return Futures.immediateVoidFuture();
                }, () -> {
                  reportedCount.incrementAndGet();
                });
              } else {
                run(() -> {
                  return Futures.submit(() -> {
                    Thread.sleep(5);
                    return Futures.immediateVoidFuture();
                  }, executor);
                }, () -> {
                  reportedCount.incrementAndGet();
                });
              }
            }
            return Futures.immediateVoidFuture();
          });
        }

        @Override
        protected void onLanded() {
          landedCount.incrementAndGet();
          System.out.println("[onLanded] landedCount:" + landedCount);
        }
      }.get();
      lf.addListener(() -> {
        System.out.println("[listener] landedCount:" + landedCount);
      }, MoreExecutors.directExecutor());
      lf.get();
      // Thread.sleep(3000);
      assertThat(reportedCount.get()).isEqualTo(desiredCount);
      assertThat(landedCount.get()).isEqualTo(1);
    } finally {
      executor.shutdown();
    }
    System.out.println("done");
  }
}
