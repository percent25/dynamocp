package io.github.awscat;

public interface InputPluginProvider {
  InputPlugin get() throws Exception;
}
