package awscat;

import java.util.function.Supplier;

import org.springframework.util.ClassUtils;

public interface OutputPluginProvider {
  default String name() {
    return ClassUtils.getShortName(getClass());
  }
  String help();
  default int mtu() {
    return -1;
  }
  boolean canActivate(String address);
  Supplier<OutputPlugin> activate(String address) throws Exception;
  // void deactivate();
}
