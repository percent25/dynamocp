package main;

import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;

public interface OutputPluginProvider {
  OutputPlugin get(String arg, ApplicationArguments args) throws Exception;
}
