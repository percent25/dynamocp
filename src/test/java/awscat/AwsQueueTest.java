package awscat;

import java.net.*;

import org.junit.jupiter.api.*;
import org.springframework.boot.*;
import org.springframework.boot.test.context.*;

import io.github.awscat.*;
import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;


// @SpringBootTest
@SpringBootTest(classes = {Main.class}, args={"arn:aws:sqs:us-east-1:000000000000:MyQueue,endpoint=http://localhost:4566"})
public class AwsQueueTest {
  
  @Test
  public void basicSmoke() throws Exception {

    SqsClient client = SqsClient.builder()
    //
    .endpointOverride(URI.create("http://localhost:4566"))
    //
    .build();

    final String queueUrl = "http://localhost:4566/000000000000/MyQueue";

    // send
    String messageBody = "asdf";
    SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody(messageBody).build();
    System.out.println(client.sendMessage(sendMessageRequest));

    // receive
    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl).build();
    System.out.println(client.receiveMessage(receiveMessageRequest));
  }

  // + target/awscat-0.0.1-SNAPSHOT.jar arn:aws:sqs:us-east-1:000000000000:MyQueue,endpoint=http://localhost:4566
  @Test
  public void receiveMessageTest() throws Exception {
    
    ApplicationArguments args = new DefaultApplicationArguments("arn:aws:sqs:us-east-1:000000000000:MyQueue,endpoint=http://localhost:4566");

  }

}
