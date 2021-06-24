package awscat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.function.Supplier;

import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

public class SystemOutTargetSupplier implements Supplier<OutputTarget> {
  @Override
  public OutputTarget get() {
    return new OutputTarget() {

      private PrintStream stdout;
      private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      @Override
      public void setUp() {
        stdout = SystemOutPluginProvider.stdout; // save
        SystemOutPluginProvider.stdout = new PrintStream(baos);
      }

      @Override
      public String address() {
        return "-";
      }

      @Override
      public JsonElement verify() {
        return new JsonStreamParser(baos.toString()).next();
      }

      @Override
      public void tearDown() {
        SystemOutPluginProvider.stdout = stdout; // restore
      }

    };
  }
}
