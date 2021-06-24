package awscat;

import org.junit.jupiter.api.Test;

public class AwsDynamoIT {
  @Test
  public void basicTargetTest() throws Exception {
    InputSourceArg source = new AwsDynamoSourceSupplier().get();
    OutputTargetArg target = new AwsDynamoTargetSupplier().get();
    new InOutRunner(source, target).run("{id:{s:abc123}}");
  }
}
