package main;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

public class SystemInInputPlugin implements InputPlugin {

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  //###TODO
  //###TODO
  //###TODO
  private final Semaphore sem = new Semaphore(15); // backpressure
  //###TODO
  //###TODO
  //###TODO

  public SystemInInputPlugin() {
  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  @Override
  public ListenableFuture<?> read() throws Exception {
    final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    try {
      List<JsonElement> partition = new ArrayList<>();
      JsonStreamParser parser = new JsonStreamParser(br);
      while (parser.hasNext()) {

        partition.add(parser.next());

        if (!parser.hasNext() || partition.size() == 1000) { // mtu
          try {
            // STEP 1
            sem.acquire();
            // STEP 2
            listener.apply(partition).addListener(()->{
              sem.release();
            }, MoreExecutors.directExecutor());
          } catch (Exception e) {
            log(e);
          }
          partition = new ArrayList<>();
        }

      }
    } finally {
      br.close();
    }
    return Futures.immediateVoidFuture();
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }
}

@Service
class SystemInInputPluginProvider implements InputPluginProvider {

  @Override
  public InputPlugin get(String arg, ApplicationArguments args) throws Exception {
    if ("-".equals(arg))
      return new SystemInInputPlugin();
    return null;
  }

}
