package io.github.awscat;

import org.springframework.util.ClassUtils;

public interface InputPluginProvider {
  default String name() {
    return ClassUtils.getShortName(getClass());
  }
  boolean canActivate();
  InputPlugin activate() throws Exception;
}
