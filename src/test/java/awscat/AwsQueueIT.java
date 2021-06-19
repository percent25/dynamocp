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
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

// @SpringBootTest
// @SpringBootTest(args={"arn:aws:sqs:us-east-1:000000000000:MyQueue,endpoint=http://localhost:4566,limit=1"})
public class AwsQueueIT {

  // beforeAll
  static String endpointUrl;
  static SqsClient client;

  // beforeEach
  private String queueName = UUID.randomUUID().toString();
  private String queueArn = String.format("arn:aws:sqs:us-east-1:000000000000:%s", queueName);
  private String queueUrl = String.format("%s/000000000000/%s", endpointUrl, queueName);

  public AwsQueueIT() {
    debug("ctor");
  }

  @BeforeAll
  public static void newClient() {

    endpointUrl = String.format("http://localhost:%s", System.getProperty("edge.port", "4566"));

    client = SqsClient.builder() //
        // .httpClient(AwsCrtAsyncHttpClient.create()) //
        .endpointOverride(URI.create(endpointUrl)) //
        .region(Region.US_EAST_1) //
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
        .build();

  }

  @BeforeEach
  public void createQueue() throws Exception {
    CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).build();
    debug(createQueueRequest);
    CreateQueueResponse createQueueResponse = client.createQueue(createQueueRequest);
    debug(createQueueResponse);
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

        // send
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
        Main.main("-", String.format("%s,endpoint=%s", queueArn, endpointUrl));

    
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SystemOutPluginProvider.stdout = new PrintStream(baos);
        Main.main(String.format("%s,endpoint=%s,limit=1", queueArn, endpointUrl));

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
    String sourceArg = String.format("%s,endpoint=%s,limit=%s", queueArn, endpointUrl, limit);
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
