package app;

import java.util.function.Function;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;

public interface InputPlugin {

  

  void setListener(Function<JsonElement, ListenableFuture<?>> listener);

  ListenableFuture<?> read() throws Exception;
  
}
