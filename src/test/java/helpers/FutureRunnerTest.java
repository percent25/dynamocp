package helpers;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.google.common.util.concurrent.*;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

public class FutureRunnerTest {

  @Test
  public void randomOneLandingTest() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      new FutureRunner() {
        int count;
        int landedCount;
        {
          run(() -> {
            for (int i = 0; i < 8; ++i) {
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
          System.out.println("onLanded:" + landedCount);
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
      new FutureRunner() {
        int count;
        int landedCount;
        {
          for (int i = 0; i < 8; ++i) {
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
                  return Futures.immediateVoidFuture();
                }, executor);
              });
            }
          }
        }
        @Override
        protected void onLanded() {
          ++landedCount;
          System.out.println("onLanded:" + landedCount);
        }
      }.get().get();
    } finally {
      executor.shutdown();
    }
    System.out.println("done");
  }

  private int landedCount;

  @Test
  public void manyLandingsTest() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      ListenableFuture<?> lf = new FutureRunner() {
        int count;
        {
          for (int i = 0; i < 8; ++i) {
            ++count;
            System.out.println("count:" + count);

            run(() -> {
            return Futures.immediateVoidFuture();
            });

            run(() -> {
              return Futures.submit(() -> {
                Thread.sleep(2000);
                return Futures.immediateVoidFuture();
              }, executor);
            });
  
          }
        }
        @Override
        protected void onLanded() {
          ++landedCount;
          System.out.println("onLanded:" + landedCount);
        }
      }.get();
      lf.addListener(() -> {
        System.out.println("listener: landedCount=" + landedCount);
        assertThat(landedCount).isEqualTo(2);
      }, MoreExecutors.directExecutor());
      lf.get();
      // Thread.sleep(3000);
    } finally {
      executor.shutdown();
    }
    System.out.println("done");
  }

  private final AtomicInteger count = new AtomicInteger();

  @Test
  public void stressTest() throws Exception {
    final int desiredCount = 10000;
    final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      ListenableFuture<?> lf = new FutureRunner() {
        {
          for (int i = 0; i < desiredCount; ++i) {
            if (new Random().nextInt() < Integer.MAX_VALUE / 2) {
              run(() -> {
                return Futures.immediateVoidFuture();
              }, ()->{
                count.incrementAndGet();
              });
            } else {
              run(() -> {
                return Futures.submit(() -> {
                  Thread.sleep(2);
                  return Futures.immediateVoidFuture();
                }, executor);
              }, ()->{
                count.incrementAndGet();
              });
            }
  
          }
        }
        @Override
        protected void onLanded() {
          ++landedCount;
        }
      }.get();
      lf.addListener(() -> {
        System.out.println("listener: landedCount=" + landedCount);
      }, MoreExecutors.directExecutor());
      lf.get();
      // Thread.sleep(3000);
      assertThat(count.get()).isEqualTo(desiredCount);
    } finally {
      executor.shutdown();
    }
    System.out.println("done");
  }
}
