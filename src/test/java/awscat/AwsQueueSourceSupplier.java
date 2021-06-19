package awscat;

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
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class AwsQueueSourceSupplier implements Supplier<SourceArg> {

  @Override
  public SourceArg get() {
    return new SourceArg() {

      String endpointUrl;
      SqsClient client;

      String queueArn;
      String queueUrl;

      @Override
      public void setUp() {
        endpointUrl = String.format("http://localhost:%s", System.getProperty("edge.port", "4566"));

        client = SqsClient.builder() //
            // .httpClient(AwsCrtAsyncHttpClient.create()) //
            .endpointOverride(URI.create(endpointUrl)) //
            .region(Region.US_EAST_1) //
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
            .build();

        String queueName = UUID.randomUUID().toString();

        queueArn = String.format("arn:aws:sqs:us-east-1:000000000000:%s", queueName);
        queueUrl = String.format("%s/000000000000/%s", endpointUrl, queueName);

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).build();
        log(createQueueRequest);
        CreateQueueResponse createQueueResponse = client.createQueue(createQueueRequest);
        log(createQueueResponse);
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
      public String sourceArg() {
        return String.format("%s,endpoint=%s,limit=1", queueArn, endpointUrl);
      }

      @Override
      public void tearDown() {
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder() //
            .queueUrl(queueUrl) //
            .build();
        log(deleteQueueRequest);
        DeleteQueueResponse deleteQueueResponse = client.deleteQueue(deleteQueueRequest);
        log(deleteQueueResponse);
      }

      void log(Object arg) {
        System.out.println(arg);
      }
    };
  }

}
