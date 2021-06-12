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

  static String endpointUrl = "http://localhost:4566";

  static SqsClient client;

  static String queueName = UUID.randomUUID().toString();
  static String queueArn = String.format("arn:aws:sqs:us-east-1:000000000000:%s", queueName);
  static String queueUrl = String.format("http://localhost:4566/000000000000/%s", queueName);

  @BeforeAll
  public static void beforeAll() {
    client = SqsClient.builder() //
        .endpointOverride(URI.create(endpointUrl)) //
        .region(Region.US_EAST_1) //
        .credentialsProvider(AnonymousCredentialsProvider.create()) //
        .build();

    CreateQueueResponse createQueueResponse = client.createQueue(CreateQueueRequest.builder().queueName(queueName).build());
    log(createQueueResponse);
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

  static void log(Object... args) {
    System.out.println(Lists.newArrayList(args));
  }

}
