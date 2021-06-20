package awscat;

import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class AwsQueueTargetSupplier implements Supplier<TargetArg> {

  @Override
  public TargetArg get() {
    return new TargetArg() {

      private SqsClient client;

      private String queueArn;
      private String queueUrl;

      @Override
      public void setUp() {

        client = AwsBuilder.create(SqsClient.builder());

        String queueName = UUID.randomUUID().toString();

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).build();
        log(createQueueRequest);
        CreateQueueResponse createQueueResponse = client.createQueue(createQueueRequest);
        log(createQueueResponse);

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
        GetQueueUrlResponse getQueueUrlResponse = client.getQueueUrl(getQueueUrlRequest);
        queueUrl = getQueueUrlResponse.queueUrl();

        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder().queueUrl(queueUrl).build();
        GetQueueAttributesResponse getQueueAttributesResponse = client.getQueueAttributes(getQueueAttributesRequest);
        queueArn = getQueueAttributesResponse.attributes().get(QueueAttributeName.QUEUE_ARN);
      }

      @Override
      public String targetArg() {
        return queueArn;
      }

      @Override
      public JsonElement verify() {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder() //
            .queueUrl(queueUrl) //
            .waitTimeSeconds(20) //
            .build();
        log(receiveMessageRequest);
        ReceiveMessageResponse receiveMessageResponse = client.receiveMessage(receiveMessageRequest);
        log(receiveMessageResponse);

        return json(receiveMessageResponse.messages().iterator().next().body());
      }

      @Override
      public void tearDown() {

        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder().queueUrl(queueUrl).build();
        log(deleteQueueRequest);
        DeleteQueueResponse deleteQueueResponse = client.deleteQueue(deleteQueueRequest);
        log(deleteQueueResponse);

      }

      private JsonElement json(String json) {
        return new JsonStreamParser(json).next();
      }

      private void log(Object arg) {
        System.out.println(getClass().getSimpleName() + arg);
      }
    };
  }

}
