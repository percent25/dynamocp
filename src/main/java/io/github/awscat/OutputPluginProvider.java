package io.github.awscat;

import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;

public interface OutputPluginProvider {
  default int mtu() {
    return 0;
  }
  boolean canActivate();
  Supplier<OutputPlugin> get() throws Exception;
}
