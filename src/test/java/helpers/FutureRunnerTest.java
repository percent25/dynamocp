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

  @Test
  public void randomOneLandingTest() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      new FutureRunner() {
        int count;
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
      }.get();
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
      };
      lf.addListener(()->{
        System.out.println("[listener] reportedCount:" + reportedCount);
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
      };
      lf.get();
      assertThat(reportedCount.get()).isEqualTo(desiredCount);
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
      };
      lf.get();
      // Thread.sleep(3000);
      assertThat(reportedCount.get()).isEqualTo(desiredCount);
    } finally {
      executor.shutdown();
    }
    System.out.println("done");
  }
}
