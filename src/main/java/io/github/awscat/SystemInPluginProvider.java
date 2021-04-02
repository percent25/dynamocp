package io.github.awscat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import helpers.FutureRunner;
import helpers.LogHelper;
import helpers.QueuePutPolicy;

class SystemInPlugin implements InputPlugin {

  private final String file;
  private final int concurrency;
  private final boolean cycle;
  private final int limit;

  private final Executor executor;

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  public SystemInPlugin(String file, int concurrency, boolean cycle, int limit) {
    debug("ctor", file, concurrency, cycle, limit);

    this.file = file;
    this.concurrency = concurrency > 0 ? concurrency : Runtime.getRuntime().availableProcessors();
    this.cycle = cycle;
    this.limit = limit;

    ThreadPoolExecutor executor = new ThreadPoolExecutor(
      0, this.concurrency, 60L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(this.concurrency));
    executor.setRejectedExecutionHandler(new QueuePutPolicy());
    this.executor = executor;
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
        //
        .add("file", file).add("concurrency", concurrency).add("cycle", cycle).add("limit", limit).toString();
  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    debug("setListener", listener);
    this.listener = listener;
  }

  @Override
  public ListenableFuture<?> read(int mtu) throws Exception {
    debug("read", "mtu", mtu);
    return new FutureRunner() {
      int count;
      int effectiveMtu = mtu > 0 ? mtu : 40000;
      List<JsonElement> partition = new ArrayList<>();
      {
        do {
          final BufferedReader br = new BufferedReader(new InputStreamReader("-".equals(file) ? System.in : new FileInputStream(file)));
          try {
            JsonStreamParser parser = new JsonStreamParser(br);
            while (parser.hasNext()) {
              ++count;
              partition.add(parser.next());
              if (!parser.hasNext() || partition.size() == effectiveMtu || count == limit) {
                run(() -> {
                  List<JsonElement> copyOfPartition = partition;
                  // var copyOfPartition = ImmutableList.copyOf(partition);
                  partition = new ArrayList<>();
                  return Futures.submitAsync(()->{
                    return listener.apply(copyOfPartition);
                  }, executor);
                });
              }
              if (count == limit)
                break;
            }
          } finally {
            br.close();
          }
          if (count == limit)
            break;
        } while (cycle);
      }
    }.get();
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }
}

// @Service
public class SystemInPluginProvider implements InputPluginProvider {

  // in.txt,c=1
  class SystemInOptions {
    int c;
    boolean cycle;
    int limit;
  }

  @Override
  public String help() {
      return "<file>[,c,cycle,limit]";
  }

  @Override
  public boolean canActivate(String arg) {
    return true;
  }

  @Override
  public InputPlugin activate(String arg) throws Exception {
    String file = Args.base(arg);
    SystemInOptions options = Args.options(arg, SystemInOptions.class);
    return new SystemInPlugin(file, options.c, options.cycle, options.limit);
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
