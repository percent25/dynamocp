package io.github.awscat;

import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;

public interface OutputPluginProvider {
  Supplier<OutputPlugin> get(String target, ApplicationArguments args) throws Exception;
}
