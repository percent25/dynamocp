package app;

import org.springframework.boot.ApplicationArguments;

public interface InputPluginProvider {
  InputPlugin get(ApplicationArguments args) throws Exception;
}
