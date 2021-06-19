package awscat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.function.Supplier;

import com.google.gson.JsonElement;

public class SystemInSourceSupplier implements Supplier<SourceArg> {
  @Override
  public SourceArg get() {
    return new SourceArg() {

      private InputStream stdin;

      @Override
      public void setUp() {
        stdin = SystemInPlugin.stdin; // save
      }

      @Override
      public void load(JsonElement jsonElement) {
        SystemInPlugin.stdin = new ByteArrayInputStream(jsonElement.toString().getBytes());
      }

      @Override
      public String sourceArg() {
        return "-";
      }

      @Override
      public void tearDown() {
        SystemInPlugin.stdin = stdin; // restore
      }

    };
  }
}
