package awscat;

import java.util.function.Function;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;

public interface InputPlugin {

  void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener);

  ListenableFuture<?> run(int mtu) throws Exception;

}
