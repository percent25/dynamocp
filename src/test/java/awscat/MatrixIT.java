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
  void setUp() throws Exception;
  void load(JsonElement jsonElement) throws Exception;
  String sourceArg();
  void tearDown() throws Exception;
}

interface TargetArg {
  void setUp() throws Exception;
  String targetArg();
  JsonElement verify() throws Exception;
  void tearDown() throws Exception;
}

public class MatrixIT {

  @Test
  public void matrixTest() throws Exception {

    final JsonElement jsonElement = json("{foo:1,bar:2}");

    Set<Supplier<SourceArg>> sources = Sets.newHashSet();
    sources.add(new AwsQueueSourceSupplier());

    Set<Supplier<TargetArg>> targets = Sets.newHashSet();
    targets.add(new AwsQueueTargetSupplier());
    targets.add(new AwsS3TargetSupplier());

    for (Supplier<SourceArg> eachSourceProvider : sources) {
      for (Supplier<TargetArg> eachTargetProvider : targets) {

        SourceArg eachSource = eachSourceProvider.get();
        TargetArg eachTarget = eachTargetProvider.get();

        eachSource.setUp();
        try {
          eachTarget.setUp();
          try {
            // STEP 1 load the source with the jsonElement
            eachSource.load(jsonElement);
            // STEP 2 invoke
            Main.main(eachSource.sourceArg(), eachTarget.targetArg());
            // STEP 3 verify the target with the jsonElement
            assertThat(eachTarget.verify()).isEqualTo(jsonElement);
          } finally {
            eachTarget.tearDown();
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
