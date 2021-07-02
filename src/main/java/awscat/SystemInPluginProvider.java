package awscat;

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import helpers.FutureRunner;
import helpers.LogHelper;
import helpers.CallerBlocksPolicy;

class SystemInPlugin implements InputPlugin {

  @VisibleForTesting
  public static InputStream stdin = System.in;

  private final String filename;
  private final int concurrency;
  private final boolean cycle;
  private final int limit;

  private final ThreadPoolExecutor executor;

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  public SystemInPlugin(String filename, int concurrency, boolean cycle, int limit) {
    debug("ctor", filename, concurrency, cycle, limit);

    this.filename = filename;
    this.concurrency = concurrency > 0 ? concurrency : Runtime.getRuntime().availableProcessors();
    this.cycle = cycle;
    this.limit = limit;

    executor = new ThreadPoolExecutor(
      0, this.concurrency, 60L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(this.concurrency));
    executor.setRejectedExecutionHandler(new CallerBlocksPolicy());
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
        //
        .add("filename", filename).add("concurrency", concurrency).add("cycle", cycle).add("limit", limit).toString();
  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    debug("setListener", listener);
    this.listener = listener;
  }

  @Override
  public ListenableFuture<?> run(int mtu) throws Exception {
    debug("read", "mtu", mtu);
    return new FutureRunner() {
      int count;
      int effectiveMtu = mtu > 0 ? mtu : 40000;
      List<JsonElement> partition = new ArrayList<>();
      {
        do {
          final BufferedReader br = new BufferedReader(new InputStreamReader("-".equals(filename) ? stdin : new FileInputStream(filename)));
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
    };
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }
}

// @Service
public class SystemInPluginProvider extends AbstractPluginProvider implements InputPluginProvider {

  // in.txt,c=1
  class SystemInOptions {
    public int c;
    public boolean cycle;
    public int limit;
  }

  public SystemInPluginProvider() {
      super("<filename>", SystemInOptions.class);
  }

  @Override
  public boolean canActivate(String address) {
    return true;
  }

  @Override
  public InputPlugin activate(String address) throws Exception {
    String filename = Addresses.base(address);
    SystemInOptions options = Addresses.options(address, SystemInOptions.class);
    return new SystemInPlugin(filename, options.c, options.cycle, options.limit);
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
