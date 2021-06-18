package awscat;

import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class AwsQueueTargetSupplier implements Supplier<TargetArg> {

  @Override
  public TargetArg get() {
    return new TargetArg() {

      // beforeAll
      String endpointUrl;
      SqsAsyncClient client;

      // beforeEach
      String queueArn;
      String queueUrl;

      @Override
      public void setUp() throws Exception {

        endpointUrl = String.format("http://localhost:%s", System.getProperty("edge.port", "4566"));

        client = SqsAsyncClient.builder() //
            // .httpClient(AwsCrtAsyncHttpClient.create()) //
            .endpointOverride(URI.create(endpointUrl)) //
            .region(Region.US_EAST_1) //
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
            .build();

        // STEP 1 setup

        String queueName = UUID.randomUUID().toString();

        queueArn = String.format("arn:aws:sqs:us-east-1:000000000000:%s", queueName);
        queueUrl = String.format("%s/000000000000/%s", endpointUrl, queueName);

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).build();
        log(createQueueRequest);
        CreateQueueResponse createQueueResponse = client.createQueue(createQueueRequest).get();
        log(createQueueResponse);

      }

      @Override
      public String targetArg() {
        return String.format("%s,endpoint=%s", queueArn, endpointUrl);
      }

      @Override
      public JsonElement verifyAndTearDown() throws Exception {

        try {

          // STEP 1 verify

          ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder() //
              .queueUrl(queueUrl) //
              .waitTimeSeconds(20) //
              .build();
          ReceiveMessageResponse receiveMessageResponse = client.receiveMessage(receiveMessageRequest).get();

          // assertThat(receiveMessageResponse.hasMessages()).isTrue();
          return json(receiveMessageResponse.messages().iterator().next().body());

        } finally {
          
          // STEP 2 teardown

          DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder().queueUrl(queueUrl).build();
          log(deleteQueueRequest);
          DeleteQueueResponse deleteQueueResponse = client.deleteQueue(deleteQueueRequest).get();
          log(deleteQueueResponse);

        }

      }

      JsonElement json(String json) {
        return new JsonStreamParser(json).next();
      }

      void log(Object arg) {
        System.out.println(arg);
      }
    };
  }

}
