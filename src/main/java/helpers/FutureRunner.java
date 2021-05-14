package helpers;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import com.google.common.util.concurrent.*;
import com.spotify.futures.*;

/**
 * Opinionated robust facade/runner for listenablefuture(s).
 */
public class FutureRunner {

  private class VoidFuture extends AbstractFuture<Void> {
    public boolean setVoid() {
      return super.set(null);
    }
    public boolean setException(Throwable throwable) {
      return super.setException(throwable);
    }
  }

  // resolved when "inFlight" transitions to zero
  private final AtomicReference<VoidFuture> facade = new AtomicReference<>();

  private final AtomicInteger inFlight = new AtomicInteger();
  private final AtomicReference<Exception> firstException = new AtomicReference<>();

  public ListenableFuture<?> get() {
    facade.compareAndSet(null, new VoidFuture());
    doFacade();
    return facade.get();
  }

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
        doFacade();
      }
    }
  }

  private void doFacade() {
    if (inFlight.get() == 0) {
      if (facade.get() != null) {
        if (firstException.get() == null)
          facade.get().setVoid();
        else
          facade.get().setException(firstException.get());
      }
    }
  }

}
