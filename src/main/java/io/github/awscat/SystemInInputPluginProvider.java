package io.github.awscat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import org.springframework.boot.ApplicationArguments;

import helpers.FutureRunner;
import helpers.LogHelper;

class SystemInInputPlugin implements InputPlugin {

  private final InputStream in;
  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  //###TODO
  //###TODO
  //###TODO
  private final int concurrency = Runtime.getRuntime().availableProcessors();
  private final Semaphore sem = new Semaphore(concurrency); // backpressure
  //###TODO
  //###TODO
  //###TODO

  public SystemInInputPlugin(InputStream in) {
    debug("ctor");
    this.in = in;
  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    debug("setListener");
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
              sem.acquire();
              run(() -> {
                return listener.apply(partition);
              }, () -> { // finally
                sem.release();
              });
              partition = new ArrayList<>();
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
  public InputPlugin get() throws Exception {
    String source = args.getNonOptionArgs().get(0);
    return new SystemInInputPlugin("-".equals(source) ? System.in : new FileInputStream(source));
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
