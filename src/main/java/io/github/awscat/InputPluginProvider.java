package io.github.awscat;

import org.springframework.util.ClassUtils;

public interface InputPluginProvider {
  default String name() {
    return ClassUtils.getShortName(getClass());
  }
  boolean canActivate(String arg);
  InputPlugin activate(String arg) throws Exception;
  // void deactivate();
}
