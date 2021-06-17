package helpers;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import com.google.common.base.*;
import com.google.common.util.concurrent.*;
import com.spotify.futures.*;

/**
 * Opinionated robust facade/runner for listenablefuture(s).
 */
public class FutureRunner extends AbstractFuture<Void> {

  private final AtomicInteger inFlight = new AtomicInteger();
  private final AtomicReference<Exception> firstException = new AtomicReference<>();

  /**
   * run
   *
   * @param <T>
   * @param request
   */
  protected <T> void run(AsyncCallable<T> request) {
    run(request, unused -> {});
  }

  /**
   * run
   *
   * @param <T>
   * @param request
   * @param response
   */
  protected <T> void run(AsyncCallable<T> request, Consumer<T> response) {
    run(request, response, e -> { throw new RuntimeException(e); });
  }

  /**
   * run
   *
   * @param <T>
   * @param request
   * @param perRequestResponseFinally
   */
  protected <T> void run(AsyncCallable<T> request, Runnable perRequestResponseFinally) {
    run(request, unused -> {}, e->{ throw new RuntimeException(e); }, perRequestResponseFinally);
  }

  /**
   * run
   *
   * @param <T>
   * @param request
   * @param response
   * @param perRequestResponseCatch
   */
  protected <T> void run(AsyncCallable<T> request, Consumer<T> response, Consumer<Exception> perRequestResponseCatch) {
    run(request, response, perRequestResponseCatch, () -> {});
  }

  /**
   * run
   *
   * @param <T>
   * @param request
   * @param response
   * @param perRequestResponseCatch
   * @param perRequestResponseFinally
   */
  protected <T> void run(AsyncCallable<T> request, Consumer<T> response, Consumer<Exception> perRequestResponseCatch, Runnable perRequestResponseFinally) {
    try {
      inFlight.incrementAndGet();
      ListenableFuture<T> lf = request.call(); // throws
      lf.addListener(() -> {
        try {
          response.accept(lf.get()); // throws
        } catch (Exception e) {
          try {
            perRequestResponseCatch.accept(e); // throws
          } catch (Exception e1) {
            doCatch(e1);
          }
        } finally {
          try {
            perRequestResponseFinally.run(); // throws
          } catch (Exception e2) {
            doCatch(e2);
          } finally {
            doFinally();
          }
        }
      }, MoreExecutors.directExecutor());
    } catch (Exception e) {
      try {
        try {
          perRequestResponseCatch.accept(e); // throws
        } catch (Exception e1) {
          doCatch(e1);
        }
      } finally {
        try {
          perRequestResponseFinally.run(); // throws
        } catch (Exception e2) {
          doCatch(e2);
        } finally {
          doFinally();
        }
      }
    }
  }

  // convenience
  // called when "running" transitions to zero
  protected void onLanded() {
    // do nothing
  }

  // convenience
  protected <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
    return CompletableFuturesExtra.toListenableFuture(cf);
  }

  // ----------------------------------------------------------------------

  private void doCatch(Exception e) {
    firstException.compareAndSet(null, e);
  }

  private void doFinally() {
    if (inFlight.decrementAndGet() == 0) {
      try {
        onLanded();
      } catch (Exception e) {
        doCatch(e);
      } finally {
        if (firstException.get() == null)
          set(Defaults.defaultValue(Void.class));
        else
          setException(firstException.get());
      }
    }
  }

}
