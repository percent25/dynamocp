package io.github.awscat;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.function.Supplier;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.springframework.boot.ApplicationArguments;

import helpers.LogHelper;

class SystemOutPlugin implements OutputPlugin {

  private final PrintStream out;

  public SystemOutPlugin(PrintStream out) {
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
public class SystemOutPluginProvider implements OutputPluginProvider {

  // out.txt,append=true
  class SystemOutOptions {
    boolean append;
    public String toString() {
      return new Gson().toJson(this);
      // return MoreObjects.toStringHelper(this).add("append", append).toString();
    }
  }

  private final ApplicationArguments args;
  private String base;
  private SystemOutOptions options;

  public SystemOutPluginProvider(ApplicationArguments args) {
    this.args = args;
    String arg = "-";
    if (args.getNonOptionArgs().size() > 1)
      arg = args.getNonOptionArgs().get(1);
    base = Args.base(arg);
    options = Args.options(arg, SystemOutOptions.class);
  }

  public String toString() {
    return MoreObjects.toStringHelper(this).add("base", base).add("options", options).toString();
  }

  @Override
  public boolean canActivate() {
    return args.getNonOptionArgs().size()>0;
  }

  @Override
  public Supplier<OutputPlugin> get() throws Exception {
    PrintStream out = "-".equals(base) ? System.out : new PrintStream(new BufferedOutputStream(new FileOutputStream(base, options.append)));
    return ()->new SystemOutPlugin(out);
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
