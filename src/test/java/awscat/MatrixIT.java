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

public class MatrixIT {

  @Test
  public void matrixTest() throws Exception {

    final JsonElement sourceJsonElement = jsonElement("{id:{s:abc123}}");

    Set<Supplier<InputSource>> sources = Sets.newHashSet();
    sources.add(new AwsDynamoSourceSupplier());
    sources.add(new AwsQueueSourceSupplier());
    sources.add(new SystemInSourceSupplier());

    Set<Supplier<OutputTarget>> targets = Sets.newHashSet();
    targets.add(new AwsDynamoTargetSupplier());
    targets.add(new AwsKinesisTargetSupplier());
    targets.add(new AwsQueueTargetSupplier());
    targets.add(new AwsS3TargetSupplier());
    targets.add(new SystemOutTargetSupplier());

    SoftAssertions.assertSoftly(softly -> {
      for (Supplier<InputSource> eachSourceProvider : sources) {
        for (Supplier<OutputTarget> eachTargetProvider : targets) {

          InputSource eachSource = eachSourceProvider.get();
          OutputTarget eachTarget = eachTargetProvider.get();

          eachSource.setUp();
          try {
            eachTarget.setUp();
            try {

              JsonElement[] targetJsonElement = new JsonElement[1];
              String sourceAddress = AwsBuilder.renderAddress(eachSource.address());
              String targetAddress = AwsBuilder.renderAddress(eachTarget.address());

              try {

                // STEP 1 load the source with the jsonElement
                eachSource.load(sourceJsonElement);

                // STEP 2 invoke
                softly.assertThatCode(() -> {
                  Main.main(sourceAddress, targetAddress);
                }).as("a=%s b=%s c=%s d=%s", eachSource, sourceAddress, eachTarget, targetAddress).doesNotThrowAnyException();

                // STEP 3 verify the target with the jsonElement
                targetJsonElement[0] = eachTarget.verify();

              } catch (Exception e) {
                e.printStackTrace();
              }

              softly //
                  .assertThat(targetJsonElement[0]) //
                  .as("a=%s b=%s c=%s d=%s", eachSource, sourceAddress, eachTarget, targetAddress) //
                  .isEqualTo(sourceJsonElement);

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
