package app;

import java.util.function.Supplier;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

public class SystemOutOutputPlugin implements OutputPlugin {

  @Override
  public ListenableFuture<?> write(Iterable<JsonElement> jsonElements) {
    for (JsonElement jsonElement : jsonElements)
      System.out.println(jsonElement);
    return Futures.immediateVoidFuture();
  }

  // @Override
  // public ListenableFuture<?> flush() {
  //   System.out.flush();
  //   return Futures.immediateVoidFuture();
  // }

}

@Service
class SystemOutOutputPluginProvider implements OutputPluginProvider {

  @Override
  public OutputPlugin get(ApplicationArguments args) throws Exception {
    String arg = args.getNonOptionArgs().get(1);
    if ("-".equals(arg))
      return new SystemOutOutputPlugin();
    return null;
  }

}
