package io.github.awscat;

import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;

public interface OutputPluginProvider {
  boolean canActivate();
  Supplier<OutputPlugin> get() throws Exception;
}
