package io.github.awscat;

import java.util.function.Supplier;

import org.springframework.util.ClassUtils;

public interface OutputPluginProvider {
  default String name() {
    return ClassUtils.getShortName(getClass());
  }
  default int mtu() {
    return -1;
  }
  boolean canActivate(String arg);
  Supplier<OutputPlugin> activate(String arg) throws Exception;
}
