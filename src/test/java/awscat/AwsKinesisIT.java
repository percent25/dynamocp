package awscat;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import org.junit.jupiter.api.Test;

public class AwsKinesisIT {

  @Test
  public void basicTargetTest() throws Exception {
    TargetArg target = new AwsKinesisTargetSupplier().get();
    target.setUp();
    try {
      JsonElement expected = jsonElement("{foo:1}");
      SystemInPlugin.stdin = new ByteArrayInputStream(expected.toString().getBytes());
      
      Main.main("-", AwsBuilder.renderAddress(target.targetArg()));

      assertThat(target.verify()).isEqualTo(expected);
    } finally {
      target.tearDown();
    }
  }

  // @Test
  // public void stressTest() throws Exception {
  //   final int limit = 20;

  //   // send
  //   for (int i = 0; i < limit; ++i) {
  //     SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody("{}").build();
  //     SendMessageResponse sendMessageResponse = client.sendMessage(sendMessageRequest);
  //     debug("stressTest", i, sendMessageRequest, sendMessageResponse);
  //   }

  //   // receive
  //   String sourceArg = String.format("%s,endpoint=%s,limit=%s", queueArn, endpointUrl, limit);
  //   Main.main(sourceArg);
  // }

  private JsonElement jsonElement(String json) {
    return new JsonStreamParser(json).next();
  }

  private void log(Object... args) {
    System.out.println(getClass().getSimpleName()+Lists.newArrayList(args));
  }

}
