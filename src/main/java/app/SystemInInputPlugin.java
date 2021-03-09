package app;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

public class SystemInInputPlugin implements InputPlugin {

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

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

        if (!parser.hasNext() || partition.size() == 20000) {
          try {
            listener.apply(partition).get(); // backpressure
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
    System.err.println(getClass().getSimpleName()+Arrays.asList(args));
  }
}

@Service
class SystemInInputPluginProvider implements InputPluginProvider {

  @Override
  public InputPlugin get(ApplicationArguments args) throws Exception {
    String arg = args.getNonOptionArgs().get(0);
    if ("-".equals(arg))
      return new SystemInInputPlugin();
    return null;
  }

}
