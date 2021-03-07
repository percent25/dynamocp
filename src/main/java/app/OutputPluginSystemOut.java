package app;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

public class OutputPluginSystemOut implements OutputPlugin {

  @Override
  public ListenableFuture<?> write(JsonElement jsonElement) {
    System.out.println(jsonElement);
    return Futures.immediateVoidFuture();
  }

  @Override
  public ListenableFuture<?> flush() {
    return Futures.immediateVoidFuture();
  }

}

@Service
class OutputPluginSystemOutProvider implements OutputPluginProvider {

  @Override
  public OutputPlugin get(ApplicationArguments args) throws Exception {
    String arg = args.getNonOptionArgs().get(1);
    if ("-".equals(arg))
      return new OutputPluginSystemOut();
    return null;
  }

}
