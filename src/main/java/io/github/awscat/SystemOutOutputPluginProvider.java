package io.github.awscat;

import java.io.BufferedOutputStream;
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
    debug("ctor");
    this.out = out;
  }

  @Override
  public ListenableFuture<?> write(JsonElement jsonElement) {
    out.println(jsonElement);
    return Futures.immediateVoidFuture();
  }

  @Override
  public ListenableFuture<?> flush() {
    out.flush();
    return Futures.immediateVoidFuture();
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}

// @Service
public class SystemOutOutputPluginProvider implements OutputPluginProvider {

  // out.txt,append=true
  class SystemOutOptions {
    boolean append;
  }

  private final ApplicationArguments args;

  public SystemOutOutputPluginProvider(ApplicationArguments args) {
    this.args = args;
  }

  @Override
  public boolean canActivate() {
    return args.getNonOptionArgs().size()>0;
  }

  class GetWork {
    boolean success;
    String failureMessage;
    String arg;
    String target;
    SystemOutOptions options;
    public String toString() {
      return getClass().getSimpleName()+new Gson().toJson(this);
    }
  }

  @Override
  public Supplier<OutputPlugin> get() throws Exception {
    GetWork work = new GetWork();
    try {
      work.arg = "-";
      if (args.getNonOptionArgs().size() > 1)
        work.arg = args.getNonOptionArgs().get(1);
      work.target = Args.base(work.arg);
      work.options = Args.options(work.arg, SystemOutOptions.class);
      PrintStream out = "-".equals(work.target) ? System.out : new PrintStream(new BufferedOutputStream(new FileOutputStream(work.target, work.options.append)));
      work.success = true;
      return () -> new SystemOutOutputPlugin(out);
    } catch (Exception e) {
      e.printStackTrace();
      work.failureMessage = ""+e;
      throw e;
    } finally {
      debug(work);
    }
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
