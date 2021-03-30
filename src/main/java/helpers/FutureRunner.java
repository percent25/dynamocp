package helpers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.futures.CompletableFuturesExtra;

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

    // resolved when "running" transitions to zero
    private final VoidFuture facade = new VoidFuture();

    private final AtomicInteger running = new AtomicInteger();
    private final AtomicReference<Exception> firstException = new AtomicReference<>();

    public ListenableFuture<?> get() {
        return facade;
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
            running.incrementAndGet();
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
    protected void onListen() {
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
        if (running.decrementAndGet() == 0) {
            try {
                //###TODO IF REALLY WANT TO GUARANTEE ONCE ONLISTEN
                //###TODO THEN CALL IF SETVOID/SETEXCEPTION RETURNS TRUE
                onListen();
                //###TODO IF REALLY WANT TO GUARANTEE ONCE ONLISTEN
                //###TODO THEN CALL IF SETVOID/SETEXCEPTION RETURNS TRUE
            } catch (Exception e) {
                doCatch(e);
            } finally {
                if (firstException.get() == null)
                    facade.setVoid();
                else
                    facade.setException(firstException.get());
            }
        }
    }

}
