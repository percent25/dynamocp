package io.github.awscat;

import org.springframework.boot.ApplicationArguments;

public interface InputPluginProvider {
  InputPlugin get(String arg, ApplicationArguments args) throws Exception;
}
