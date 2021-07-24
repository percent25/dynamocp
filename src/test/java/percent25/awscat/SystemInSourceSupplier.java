package percent25.awscat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.function.Supplier;

import com.google.gson.JsonElement;

import percent25.awscat.SystemInPlugin;

public class SystemInSourceSupplier implements Supplier<InputSource> {
  @Override
  public InputSource get() {
    return new InputSource() {

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
      public String address() {
        return "-";
      }

      @Override
      public void tearDown() {
        SystemInPlugin.stdin = stdin; // restore
      }

    };
  }
}
