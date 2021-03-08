package app;

import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;

public interface OutputPluginProvider {
  Supplier<OutputPlugin> get(ApplicationArguments args) throws Exception;
}
