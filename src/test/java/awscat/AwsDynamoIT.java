package awscat;

import org.junit.jupiter.api.Test;

public class AwsDynamoIT {
  @Test
  public void basicTargetTest() throws Exception {
    InputSource source = new AwsDynamoSourceSupplier().get();
    OutputTarget target = new AwsDynamoTargetSupplier().get();
    new InOutRunner(source, target).run("{id:{s:abc123}}");
  }
}
