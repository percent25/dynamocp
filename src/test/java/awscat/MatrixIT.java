package awscat;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import org.junit.jupiter.api.Test;

interface SourceArg {
  void setUpAndLoad(JsonElement jsonElement) throws Exception;
  String sourceArg();
  void tearDown() throws Exception;
}

interface TargetArg {
  void setUp() throws Exception;
  String targetArg();
  JsonElement verifyAndTearDown() throws Exception;
}

public class MatrixIT {

  @Test
  public void matrixTest() throws Exception {

    Set<Supplier<SourceArg>> sources = Sets.newHashSet();
    sources.add(new AwsQueueSourceSupplier());

    Set<Supplier<TargetArg>> targets = Sets.newHashSet();
    targets.add(new AwsQueueTargetSupplier());

    for (Supplier<SourceArg> eachSourceProvider : sources) {
      for (Supplier<TargetArg> eachTargetProvider : targets) {

        final JsonElement jsonElement = json("{foo:1,bar:2}");

        SourceArg eachSource = eachSourceProvider.get();
        TargetArg eachTarget = eachTargetProvider.get();

        eachSource.setUpAndLoad(jsonElement);
        try {
          eachTarget.setUp();
          try {
            Main.main(eachSource.sourceArg(), eachTarget.targetArg());
          } finally {
            assertThat(eachTarget.verifyAndTearDown()).isEqualTo(jsonElement);
          }
        } finally {
          eachSource.tearDown();
        }

      }
    }

  }

  private JsonElement json(String json) {
    return new JsonStreamParser(json).next();
  }

  private void log(Object... args) {
    System.out.println(Lists.newArrayList(args));
  }

}
