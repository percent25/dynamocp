package percent25.awscat;

import java.util.function.Function;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;

public interface InputPlugin {

  // start
  ListenableFuture<?> run(int mtuHint) throws Exception;

  // close
  // void closeNonBlocking();

  void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener);

}
