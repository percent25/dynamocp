package helpers;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.Futures;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

public class FutureRunnerTest {

  @Test
  public void futureRunnerTest() throws Exception {

    final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
        Object result = new FutureRunner() {
            int count;
            int listenCount;
            {
                run(() -> {
                    for (int i = 0; i < 8; ++i) {
                        ++count;
                        System.out.println("count:"+count);
                        // run(() -> {
                        //     return Futures.immediateVoidFuture();
                        // });
                        if (new Random().nextInt() < Integer.MAX_VALUE / 2) {
                            run(() -> {
                                return Futures.immediateVoidFuture();
                            });
                        } else {
                            run(() -> {
                                return Futures.submit(()->{
                                    return Futures.immediateVoidFuture();
                                }, executor);
                            });
                        }
                    }
                    return Futures.immediateVoidFuture();
                });
            }
            @Override
            protected void onListen() {
                ++listenCount;
                System.out.println("onListen:"+listenCount);
                assertThat(listenCount).isEqualTo(1);
            }
        }.get().get();
        System.out.println(result);
    } finally {
        executor.shutdown();
    }

  }
  
}
