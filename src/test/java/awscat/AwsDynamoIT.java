package awscat;

import static org.assertj.core.api.Assertions.*;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import org.junit.jupiter.api.Test;

public class AwsDynamoIT {

  @Test
  public void basicTargetTest() throws Exception {
    JsonElement jsonElement = jsonElement("{id:{s:abc123}}");

    SourceArg source = new AwsDynamoSourceSupplier().get();
    TargetArg target = new SystemOutTargetSupplier().get();

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
