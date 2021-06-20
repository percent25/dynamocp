package awscat;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import helpers.LogHelper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class AwsQueueIT {

  // beforeAll
  static SqsClient client;

  @BeforeAll
  public static void newClient() {
    client = AwsBuilder.create(SqsClient.builder());
  }

  // beforeEach
  private String queueArn;
  private String queueUrl;

  @BeforeEach
  public void createQueue() throws Exception {
    String queueName = UUID.randomUUID().toString();

    CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).build();
    debug(createQueueRequest);
    CreateQueueResponse createQueueResponse = client.createQueue(createQueueRequest);
    debug(createQueueResponse);

    GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
    GetQueueUrlResponse getQueueUrlResponse = client.getQueueUrl(getQueueUrlRequest);
    queueUrl = getQueueUrlResponse.queueUrl();

    GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder() //
        .queueUrl(queueUrl) //
        .attributeNames(QueueAttributeName.ALL) //
        .build();
    GetQueueAttributesResponse getQueueAttributesResponse = client.getQueueAttributes(getQueueAttributesRequest);
    queueArn = getQueueAttributesResponse.attributes().get(QueueAttributeName.QUEUE_ARN);

  }

  @AfterEach
  public void deleteQueue() throws Exception {
    DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder().queueUrl(queueUrl).build();
    debug(deleteQueueRequest);
    DeleteQueueResponse deleteQueueResponse = client.deleteQueue(deleteQueueRequest);
    debug(deleteQueueResponse);
  }

  /**
   * 
   */
  @Test
  public void basicSendTest() {
    // log("verifyTest");

    // Main.main(String.format("%s,endpoint=%s,limit=1", queueArn, endpointUrl));

        // // receive
        // ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl).build();
        // log(client.receiveMessage(receiveMessageRequest));


    // ### TODO here is where we would verify that awscat consumed
  }

  @Test
  public void basicReceiveTest() throws Exception {

        // STEP 1 send

        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        JsonElement json = json("{foo:1}");
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
              // SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody(messageBody).build();
              // log(sendMessageRequest);
              // SendMessageResponse sendMessageResponse = client.sendMessage(sendMessageRequest).get();
              // log(sendMessageResponse);

        SystemInPlugin.stdin = new ByteArrayInputStream(json.toString().getBytes());
        String targetAddress = AwsBuilder.renderAddress(queueArn);
        Main.main("-", targetAddress);

        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken

        // STEP 2 receive

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SystemOutPluginProvider.stdout = new PrintStream(baos);
        
        String sourceAddress = AwsBuilder.renderAddress(String.format("%s,limit=1", queueArn));
        Main.main(sourceAddress, "-");

        assertThat(json(baos.toString())).isEqualTo(json);

        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken

  }

  @Test
  public void stressTest() throws Exception {
    final int limit = 20;

    // send
    for (int i = 0; i < limit; ++i) {
      SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody("{}").build();
      SendMessageResponse sendMessageResponse = client.sendMessage(sendMessageRequest);
      debug("stressTest", i, sendMessageRequest, sendMessageResponse);
    }

    // receive
    String sourceArg = AwsBuilder.renderAddress(String.format("%s,limit=%s", queueArn, limit));
    Main.main(sourceArg);
  }

  private JsonElement json(String json) {
    return new JsonStreamParser(json).next();
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
    // System.out.println(Lists.newArrayList(args));
  }

}
