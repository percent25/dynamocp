package io.github.awscat;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
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

  class SystemOutOptions {
    boolean append;
  }

  class GetWork {
    boolean success;
    String failureMessage;
    final String arg;
    String target;
    SystemOutOptions options;
    public GetWork(String arg) {
      this.arg = arg;
    }
    public String toString() {
      return getClass().getSimpleName()+new Gson().toJson(this);
    }
  }

  @Override
  public Supplier<OutputPlugin> get(String arg, ApplicationArguments args) throws Exception {
    GetWork work = new GetWork(arg);
    try {
      work.target = Args.parseArg(work.arg);
      work.options = Options.parse(work.arg, SystemOutOptions.class);
      PrintStream out = "-".equals(work.target) ? System.out : new PrintStream(new FileOutputStream(work.target, work.options.append));
      work.success = true;
      return () -> {
        try {
          return new SystemOutOutputPlugin(out);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };
    } catch (Exception e) {
      e.printStackTrace();
      work.failureMessage = ""+e;
      throw e;
    } finally {
      log(work);
    }
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}
