package awscat;

import java.net.*;
import java.util.*;

import org.junit.jupiter.api.*;
import org.springframework.boot.*;
import org.springframework.boot.test.context.*;

import awscat.*;
import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;


// @SpringBootTest
// @SpringBootTest(args={"arn:aws:sqs:us-east-1:000000000000:MyQueue,endpoint=http://localhost:4566,limit=1"})
public class AwsQueueTest {

  static String endpointUrl = "http://localhost:4566";

  static SqsClient client = SqsClient.builder()
  //
  .endpointOverride(URI.create(endpointUrl))
  //
  .build();

  static String queueName = UUID.randomUUID().toString();
  static String queueArn = String.format("arn:aws:sqs:us-east-1:000000000000:%s", queueName);
  static String queueUrl = String.format("http://localhost:4566/000000000000/%s", queueName);

  @BeforeAll
  public static void beforeAll() {
    log(client.createQueue(CreateQueueRequest.builder().queueName(queueName).build()));

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
  public void receiveTest() {

        // send
        String messageBody = "asdf1 asdf2";
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody(messageBody).build();
        log(client.sendMessage(sendMessageRequest));
    
    Main.main(String.format("%s,endpoint=%s,limit=1", queueArn, endpointUrl));

    // ### TODO here is where we would verify that awscat consumed
  }

  static void log(Object... args) {
    System.out.println(args);
  }

}
