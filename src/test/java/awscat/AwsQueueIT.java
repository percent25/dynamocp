package awscat;

import java.net.*;
import java.util.*;

import com.google.common.collect.*;

import org.junit.jupiter.api.*;
import org.springframework.boot.*;
import org.springframework.boot.test.context.*;

import awscat.*;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.async.*;
import software.amazon.awssdk.http.crt.*;
import software.amazon.awssdk.regions.*;
import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;


// @SpringBootTest
// @SpringBootTest(args={"arn:aws:sqs:us-east-1:000000000000:MyQueue,endpoint=http://localhost:4566,limit=1"})
public class AwsQueueIT {

  static SqsAsyncClient client;
  static String endpointUrl = "http://localhost:4566";

  private String queueName = UUID.randomUUID().toString();
  private String queueArn = String.format("arn:aws:sqs:us-east-1:000000000000:%s", queueName);
  private String queueUrl = String.format("http://localhost:4566/000000000000/%s", queueName);

  public AwsQueueIT() {
    log("ctor");
  }

  @BeforeAll
  public static void newClient() {
    client = SqsAsyncClient.builder() //
        .httpClient(AwsCrtAsyncHttpClient.create()) //
        .endpointOverride(URI.create(endpointUrl)) //
        .region(Region.US_EAST_1) //
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
        .build();
  }

  @BeforeEach
  public void createQueue() throws Exception {
    CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).build();
    log(createQueueRequest);
    CreateQueueResponse createQueueResponse = client.createQueue(createQueueRequest).get();
    log(createQueueResponse);
  }

  @AfterEach
  public void deleteQueue() throws Exception {
    DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder().queueUrl(queueUrl).build();
    log(deleteQueueRequest);
    DeleteQueueResponse deleteQueueResponse = client.deleteQueue(deleteQueueRequest).get();
    log(deleteQueueResponse);
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
        String messageBody = "{}";
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody(messageBody).build();
        log(sendMessageRequest);
        SendMessageResponse sendMessageResponse = client.sendMessage(sendMessageRequest).get();
        log(sendMessageResponse);
    
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        Main.main(String.format("%s,endpoint=%s,limit=1", queueArn, endpointUrl));
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken

    // ### TODO here is where we would verify that awscat consumed
  }

  @Test
  public void stressTest() throws Exception {
    final int limit = 100;

    // send
    for (int i = 0; i < limit; ++i) {
      SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody("{}").build();
      SendMessageResponse sendMessageResponse = client.sendMessage(sendMessageRequest).get();
      log("stressTest", i, sendMessageRequest, sendMessageResponse);
    }

    // receive
    Main.main(String.format("%s,endpoint=%s,limit=%s", queueArn, endpointUrl, limit));
  }

  static void log(Object... args) {
    System.out.println(Lists.newArrayList(args));
  }

}
