package io.github.awscat.plugins;

import java.util.function.Supplier;

import io.github.awscat.OutputPlugin;
import io.github.awscat.OutputPluginProvider;

// delegates toString to the provider
public class AbstractSupplier implements Supplier<OutputPlugin> {

  private final OutputPluginProvider provider;
  private final Supplier<OutputPlugin> supplier;

  AbstractSupplier(OutputPluginProvider provider, Supplier<OutputPlugin> supplier) {
    this.provider = provider;
    this.supplier = supplier;
  }

  public String toString() {
    return provider.toString();
  }

  @Override
  public OutputPlugin get() {
    return supplier.get();
  }

}
