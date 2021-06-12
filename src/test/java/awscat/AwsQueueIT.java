package awscat;

import java.net.*;
import java.util.*;

import com.google.common.collect.*;

import org.junit.jupiter.api.*;
import org.springframework.boot.*;
import org.springframework.boot.test.context.*;

import awscat.*;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.*;
import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;


// @SpringBootTest
// @SpringBootTest(args={"arn:aws:sqs:us-east-1:000000000000:MyQueue,endpoint=http://localhost:4566,limit=1"})
public class AwsQueueIT {

  static SqsClient client;
  static String endpointUrl = "http://localhost:4566";

  private String queueName = UUID.randomUUID().toString();
  private String queueArn = String.format("arn:aws:sqs:us-east-1:000000000000:%s", queueName);
  private String queueUrl = String.format("http://localhost:4566/000000000000/%s", queueName);

  @BeforeAll
  public static void newClient() {
    client = SqsClient.builder() //
        .endpointOverride(URI.create(endpointUrl)) //
        .region(Region.US_EAST_1) //
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
        .build();
  }

  @BeforeEach
  public void newQueue() {
    CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).build();
    CreateQueueResponse createQueueResponse = client.createQueue(createQueueRequest);
    log("newQueue", createQueueRequest, createQueueResponse);
  }

  @Test
  public void sendTest() {
    // log("verifyTest");

    // Main.main(String.format("%s,endpoint=%s,limit=1", queueArn, endpointUrl));

        // // receive
        // ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl).build();
        // log(client.receiveMessage(receiveMessageRequest));


    // ### TODO here is where we would verify that awscat consumed
  }

  @Test
  public void receiveTest() throws Exception {

        // send
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        String messageBody = "{}{}";
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody(messageBody).build();
        log(client.sendMessage(sendMessageRequest));
    
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
    for (int i = 0; i < limit; ++i)
      log(i, client.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("{}").build()));

    // receive
    Main.main(String.format("%s,endpoint=%s,limit=%s", queueArn, endpointUrl, limit));
  }

  static void log(Object... args) {
    System.out.println(Lists.newArrayList(args));
  }

}
