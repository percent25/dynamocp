package main;

import java.util.function.Function;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;

public interface InputPlugin {

  ListenableFuture<?> read() throws Exception;

  void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener);
  
}
