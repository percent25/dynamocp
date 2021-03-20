package helpers;

import java.security.SecureRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.Futures;
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
      
    private final VoidFuture facade = new VoidFuture();
    private final AtomicInteger running = new AtomicInteger(1);
    private final AtomicReference<Exception> firstException = new AtomicReference<>();
    // private final List<ListenableFuture<?>> insideFutures = Collections.synchronizedList(new ArrayList<>());

    /**
     * ctor
     */
    public FutureRunner() {
        // facade.addListener(() -> {
        //     if (facade.isCancelled()) {
        //         synchronized (insideFutures) {
        //             insideFutures.forEach(insideFuture -> insideFuture.cancel(true));
        //         }
        //     }
        // }, MoreExecutors.directExecutor());
    }

    public ListenableFuture<?> get() {
        // synchronized (lock)
        {
            // --running;
            // running.decrementAndGet();
            doFinally(); // safety
            return facade;
        }
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
        // synchronized (lock)
        {
            try {
                ListenableFuture<T> lf = request.call(); // throws
                running.incrementAndGet();
                // insideFutures.add(lf);
                lf.addListener(() -> {
                    // synchronized (lock)
                    {
                        // running.decrementAndGet();
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
                    }
                }, MoreExecutors.directExecutor());
            } catch (Exception e) {
                // insideFutures.add(Futures.immediateFailedFuture(e));
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
    }

    // convenience
    protected void onListen() {
        // do nothing
    }

    // convenience
    protected <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
        return CompletableFuturesExtra.toListenableFuture(cf);
    }

    // ----------------------------------------------------------------------

    private void doCatch(Exception e) {
        if (firstException.compareAndSet(null, e)) {

        }
    }

    private void doFinally() {
        if (running.decrementAndGet() == 0) {
            try {
                onListen();
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

    public static void main(String... args) throws Exception {
        var executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try {
            var lf = new FutureRunner() {
                int count;
                int listenCount;
                {
                    // run(() -> {
                        for (int i = 0; i < 8; ++i) {
                            ++count;
                            System.out.println("count:"+count);
                            // run(() -> {
                            //     return Futures.immediateVoidFuture();
                            // });
                            if (new SecureRandom().nextInt() < Integer.MAX_VALUE / 2) {
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
                        // return Futures.immediateVoidFuture();
                    // });
                }
                @Override
                protected void onListen() {
                    ++listenCount;
                    System.out.println("onListen:"+listenCount);
                }
            }.get().get();
            System.out.println(lf);
        } finally {
            executor.shutdown();
        }
    }

}
