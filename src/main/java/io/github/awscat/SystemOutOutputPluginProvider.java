package io.github.awscat;

import java.io.PrintStream;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;

import org.springframework.boot.ApplicationArguments;

import helpers.LogHelper;

class SystemOutOutputPlugin implements OutputPlugin {

  private final PrintStream out;

  public SystemOutOutputPlugin(PrintStream out) {
    log("ctor");
    this.out = out;
  }

  @Override
  public ListenableFuture<?> write(Iterable<JsonElement> jsonElements) {
    try {
      for (JsonElement jsonElement : jsonElements)
        out.println(jsonElement);
    } finally {
      out.flush();
    }
    return Futures.immediateVoidFuture();
  }

  // @Override
  // public ListenableFuture<?> flush() {
  //   System.out.flush();
  //   return Futures.immediateVoidFuture();
  // }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}

// @Service
public class SystemOutOutputPluginProvider implements OutputPluginProvider {

  @Override
  public Supplier<OutputPlugin> get(String target, ApplicationArguments args) throws Exception {
    log("get", target);
    PrintStream out = "-".equals(target) ? System.out : new PrintStream(target);
    return () -> {
      try {
        return new SystemOutOutputPlugin(out);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}
