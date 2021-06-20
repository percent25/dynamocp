package awscat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import org.junit.jupiter.api.Test;

public class SystemInOutTest {

  @Test
  public void testSystemInOut() {
    testInOut(new SystemInSourceSupplier().get(), new SystemOutTargetSupplier().get());
  }

  private void testInOut(SourceArg source, TargetArg target) {
    JsonElement jsonElement = jsonElement("{foo:1}");
    source.setUp();
    try {
      target.setUp();
      try {
        JsonElement[] receivedJsonElement = new JsonElement[1];
        try {
          // load
          source.load(jsonElement);
          // invoke
          String sourceAddress = AwsBuilder.renderAddress(source.sourceArg());
          String targetAddress = AwsBuilder.renderAddress(target.targetArg());
          assertThatCode(()->{
            Main.main(sourceAddress, targetAddress);
          }).doesNotThrowAnyException();
          // verify
          receivedJsonElement[0] = target.verify();
        } catch (Exception e) {
          e.printStackTrace();
        }
        assertThat(receivedJsonElement[0]).isEqualTo(jsonElement);
      } finally {
        target.tearDown();
      }
    } finally {
      source.tearDown();
    }
  }

  private JsonElement jsonElement(String json) {
    return new JsonStreamParser(json).next();
  }

  private void log(Object... args) {
    System.out.println(getClass().getSimpleName()+Lists.newArrayList(args));
  }
}
