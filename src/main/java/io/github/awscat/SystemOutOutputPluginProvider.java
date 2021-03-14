package io.github.awscat;

import java.io.File;
import java.io.PrintStream;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;

import io.github.awscat.OutputPlugin;
import io.github.awscat.OutputPluginProvider;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

class SystemOutOutputPlugin implements OutputPlugin {

  private final PrintStream out;

  public SystemOutOutputPlugin(PrintStream out) {
    this.out = out;
  }

  @Override
  public ListenableFuture<?> write(Iterable<JsonElement> jsonElements) {
    for (JsonElement jsonElement : jsonElements)
      out.println(jsonElement);
    return Futures.immediateVoidFuture();
  }

  // @Override
  // public ListenableFuture<?> flush() {
  //   System.out.flush();
  //   return Futures.immediateVoidFuture();
  // }

}

@Service
public class SystemOutOutputPluginProvider implements OutputPluginProvider {

  @Override
  public Supplier<OutputPlugin> get(String arg, ApplicationArguments args) throws Exception {
    if ("-".equals(arg))
      return ()->new SystemOutOutputPlugin(System.out);
    // File f = new File(arg);
    // if (f.exists())
    //   return new SystemOutOutputPlugin(new PrintStream(f));
    return null;
  }

}
