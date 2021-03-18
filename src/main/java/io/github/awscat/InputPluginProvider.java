package io.github.awscat;

public interface InputPluginProvider {
  boolean canActivate();
  InputPlugin get() throws Exception;
}
