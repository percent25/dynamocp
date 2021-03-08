package app;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;

public interface OutputPlugin {

  ListenableFuture<?> write(Iterable<JsonElement> jsonElements);

  // ListenableFuture<?> flush();
  
}
