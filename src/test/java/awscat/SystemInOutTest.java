package awscat;

import org.junit.jupiter.api.Test;

public class SystemInOutTest {

  @Test
  public void testSystemInOut() {
    new InOutRunner(new SystemInSourceSupplier().get(), new SystemOutTargetSupplier().get()).run("{foo:1}");
  }

}
