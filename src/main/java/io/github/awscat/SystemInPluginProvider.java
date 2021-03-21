package io.github.awscat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
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

import org.springframework.boot.ApplicationArguments;

import helpers.FutureRunner;
import helpers.LogHelper;
import helpers.QueuePutPolicy;

class SystemInPlugin implements InputPlugin {

  private final InputStream in;
  private final int concurrency;
  private final Executor executor;
  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  public SystemInPlugin(InputStream in, int concurrency) {
    debug("ctor", in, concurrency);
    this.in = in;
    this.concurrency = concurrency > 0 ? concurrency : Runtime.getRuntime().availableProcessors();
    var executor = new ThreadPoolExecutor(
      0, this.concurrency, 60L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(this.concurrency));
    executor.setRejectedExecutionHandler(new QueuePutPolicy());
    this.executor = executor;
  }

  public String toString() {
    return MoreObjects.toStringHelper(this).add("in", in).add("concurrency", concurrency).toString();
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
      int effectiveMtu = mtu > 0 ? mtu : 40000;
      List<JsonElement> partition = new ArrayList<>();
      {
        final BufferedReader br = new BufferedReader(new InputStreamReader(in));
        try {
          JsonStreamParser parser = new JsonStreamParser(br);
          while (parser.hasNext()) {
            partition.add(parser.next());
            if (!parser.hasNext() || partition.size() == effectiveMtu) {
              run(() -> {
                var copyOfPartition = partition;
                // var copyOfPartition = ImmutableList.copyOf(partition);
                partition = new ArrayList<>();
                return Futures.submitAsync(()->{
                  return listener.apply(copyOfPartition);
                }, executor);
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
public class SystemInPluginProvider implements InputPluginProvider {

  // in.txt,c=1
  class SystemInOptions {
    int c;
  }

  private final ApplicationArguments args;

  public SystemInPluginProvider(ApplicationArguments args) {
    this.args = args;
  }

  @Override
  public boolean canActivate() {
    return args.getNonOptionArgs().size()>0;
  }

  @Override
  public InputPlugin get() throws Exception {
    var arg = args.getNonOptionArgs().get(0);
    var base = Args.base(arg);
    var options = Args.options(arg, SystemInOptions.class);
    return new SystemInPlugin("-".equals(base) ? System.in : new FileInputStream(base), options.c);
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
