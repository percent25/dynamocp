package io.github.awscat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.MoreCollectors;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import org.springframework.boot.ApplicationArguments;

import helpers.QueuePutPolicy;
import helpers.FutureRunner;
import helpers.LogHelper;

class SystemInInputPlugin implements InputPlugin {

  private final InputStream in;
  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  //###TODO
  //###TODO
  //###TODO
  private final int concurrency = Runtime.getRuntime().availableProcessors();
  // private final Semaphore sem = new Semaphore(concurrency); // backpressure
  private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
    0, concurrency, 60L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(concurrency));
  //###TODO
  //###TODO
  //###TODO

  public SystemInInputPlugin(InputStream in) {
    debug("ctor", in);
    this.in = in;
    executor.setRejectedExecutionHandler(new QueuePutPolicy());
  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    debug("setListener", listener);
    this.listener = listener;
  }

  @Override
  public ListenableFuture<?> read() throws Exception {
    debug("read");
    return new FutureRunner() {
      List<JsonElement> partition = new ArrayList<>();
      {
        final BufferedReader br = new BufferedReader(new InputStreamReader(in));
        try {
          JsonStreamParser parser = new JsonStreamParser(br);
          while (parser.hasNext()) {
            partition.add(parser.next());
            //###TODDO 1000
            //###TODDO 1000
            //###TODDO 1000
            if (!parser.hasNext() || partition.size() == 1000) { // mtu
              // sem.acquire();
              run(() -> {
                var copyOfPartition = partition;
                // var copyOfPartition = ImmutableList.copyOf(partition);
                partition = new ArrayList<>();
                return Futures.submitAsync(()->{
                  return listener.apply(copyOfPartition);
                }, executor);
              }, () -> { // finally
                // sem.release();
              });
            }
          }
        } finally {
          br.close();
        }
      }
    }.get();
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }
}

// @Service
public class SystemInInputPluginProvider implements InputPluginProvider {

  private final ApplicationArguments args;

  public SystemInInputPluginProvider(ApplicationArguments args) {
    this.args = args;
  }

  @Override
  public boolean canActivate() {
    return args.getNonOptionArgs().size()>0;
  }

  @Override
  public InputPlugin get() throws Exception {
    String source = args.getNonOptionArgs().get(0);
    return new SystemInInputPlugin("-".equals(source) ? System.in : new FileInputStream(source));
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
