package app;

import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;

public interface OutputPluginProvider {
  OutputPlugin get(ApplicationArguments args) throws Exception;
}
