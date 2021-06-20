package awscat;

import org.junit.jupiter.api.Test;

public class AwsDynamoIT {
  @Test
  public void basicTargetTest() throws Exception {
    SourceArg source = new AwsDynamoSourceSupplier().get();
    TargetArg target = new AwsDynamoTargetSupplier().get();
    new InOutRunner(source, target).run("{id:{s:abc123}}");
  }
}
