package percent25.awscat;

import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.gson.JsonElement;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class AwsQueueSourceSupplier implements Supplier<InputSource> {

  @Override
  public InputSource get() {
    return new InputSource() {
      SqsClient client;

      String queueArn;
      String queueUrl;

      @Override
      public void setUp() {
        client = AwsBuilder.build(SqsClient.builder());

        String queueName = UUID.randomUUID().toString();

        CreateQueueRequest createRequest = CreateQueueRequest.builder().queueName(queueName).build();
        log(createRequest);
        CreateQueueResponse createResponse = client.createQueue(createRequest);
        log(createResponse);
        
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

      @Override
      public void load(JsonElement jsonElement) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder() //
            .queueUrl(queueUrl) //
            .messageBody(jsonElement.toString()) //
            .build();
        log(sendMessageRequest);
        SendMessageResponse sendMessageResponse = client.sendMessage(sendMessageRequest);
        log(sendMessageResponse);
      }

      @Override
      public String address() {
        return String.format("%s,limit=1", queueArn);
      }

      @Override
      public void tearDown() {
        DeleteQueueRequest deleteRequest = DeleteQueueRequest.builder() //
            .queueUrl(queueUrl) //
            .build();
        log(deleteRequest);
        DeleteQueueResponse deleteResponse = client.deleteQueue(deleteRequest);
        log(deleteResponse);
      }

      private void log(Object arg) {
        System.out.println(getClass().getSimpleName() + arg);
      }
    };
  }

}
