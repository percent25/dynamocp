package awscat;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

interface SourceArg {
  void setUp();
  void load(JsonElement jsonElement);
  String sourceArg();
  void tearDown();
}

interface TargetArg {
  void setUp();
  String targetArg();
  JsonElement verify();
  void tearDown();
}

public class MatrixIT {

  @Test
  public void matrixTest() throws Exception {

    final JsonElement sourceJsonElement = jsonElement("{foo:1,bar:2}");

    Set<Supplier<SourceArg>> sources = Sets.newHashSet();
    sources.add(new AwsQueueSourceSupplier());

    Set<Supplier<TargetArg>> targets = Sets.newHashSet();
    targets.add(new AwsKinesisTargetSupplier());
    targets.add(new AwsQueueTargetSupplier());
    targets.add(new AwsS3TargetSupplier());

    SoftAssertions.assertSoftly(softly -> {
      for (Supplier<SourceArg> eachSourceProvider : sources) {
        for (Supplier<TargetArg> eachTargetProvider : targets) {

          SourceArg eachSource = eachSourceProvider.get();
          TargetArg eachTarget = eachTargetProvider.get();

          eachSource.setUp();
          try {
            eachTarget.setUp();
            try {

              JsonElement[] targetJsonElement = new JsonElement[1];

              try {

                // STEP 1 load the source with the jsonElement
                eachSource.load(sourceJsonElement);

                // STEP 2 invoke
                softly.assertThatCode(() -> {
                  Main.main(eachSource.sourceArg(), eachTarget.targetArg());
                }).doesNotThrowAnyException();

                // STEP 3 verify the target with the jsonElement
                targetJsonElement[0] = eachTarget.verify();

              } catch (Exception e) {
                e.printStackTrace();
              }

              softly.assertThat(targetJsonElement[0]).isEqualTo(sourceJsonElement);

            } finally {
              eachTarget.tearDown();
            }
          } finally {
            eachSource.tearDown();
          }

        }
      }
    });
  }

  private JsonElement jsonElement(String json) {
    return new JsonStreamParser(json).next();
  }

  private void log(Object... args) {
    System.out.println(Lists.newArrayList(args));
  }

}
