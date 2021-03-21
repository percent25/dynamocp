package io.github.awscat;

public interface InputPluginProvider {
  boolean canActivate();
  InputPlugin activate() throws Exception;
}
