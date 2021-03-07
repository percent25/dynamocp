package app;

import org.springframework.boot.ApplicationArguments;

public interface OutputPluginProvider {
  OutputPlugin get(ApplicationArguments args) throws Exception;
}
