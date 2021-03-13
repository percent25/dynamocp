package main;

import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;

public interface OutputPluginProvider {
  Supplier<OutputPlugin> get(String arg, ApplicationArguments args) throws Exception;
}
