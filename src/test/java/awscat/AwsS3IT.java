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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

// @SpringBootTest
// @SpringBootTest(args={"arn:aws:sqs:us-east-1:000000000000:MyQueue,endpoint=http://localhost:4566,limit=1"})
public class AwsS3IT {

  // beforeAll
  static String endpointUrl;
  static S3Client client;

  // beforeEach
  private String bucket = UUID.randomUUID().toString();
  // private String queueArn = String.format("arn:aws:sqs:us-east-1:000000000000:%s", queueName);
  // private String queueUrl = String.format("%s/000000000000/%s", endpointUrl, queueName);

  public AwsS3IT() {
    log("ctor");
  }

  @BeforeAll
  public static void newClient() {

    endpointUrl = String.format("http://localhost:%s", System.getProperty("edge.port", "4566"));

    client = S3Client.builder() //
        // .httpClient(AwsCrtAsyncHttpClient.create()) //
        .endpointOverride(URI.create(endpointUrl)) //
        .region(Region.US_EAST_1) //
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
        .build();

  }

  @BeforeEach
  public void createBucket() throws Exception {
    CreateBucketRequest request = CreateBucketRequest.builder().bucket(bucket).build();
    log(request);
    CreateBucketResponse response = client.createBucket(request);
    log(response);
}

  @AfterEach
  public void deleteBucket() throws Exception {
    ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucket).build();
    ListObjectsV2Response response = client.listObjectsV2(request);

    for (S3Object s3object : response.contents()) {
      DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(s3object.key()).build();
      log(deleteObjectRequest);
      DeleteObjectResponse deleteObjectResponse = client.deleteObject(deleteObjectRequest);
      log(deleteObjectResponse);  
    }

    DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
    log(deleteBucketRequest);
    DeleteBucketResponse deleteBucketResponse = client.deleteBucket(deleteBucketRequest);
    log(deleteBucketResponse);
  }

  @Test
  public void basicTargetTest() throws Exception {

        // send
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        JsonElement jsonElement = jsonElement("{foo:1}");
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
              // SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody(messageBody).build();
              // log(sendMessageRequest);
              // SendMessageResponse sendMessageResponse = client.sendMessage(sendMessageRequest).get();
              // log(sendMessageResponse);

              // STEP 1 load

        SystemInPlugin.stdin = new ByteArrayInputStream(jsonElement.toString().getBytes());
        Main.main("-", String.format("s3://%s/myprefix,endpoint=%s", bucket, endpointUrl));
    
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken

        // STEP 2 verify

            // ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // Systems.stdout = new PrintStream(baos);
            // Main.main(String.format("s3://%s/myprefix,endpoint=%s,limit=1", bucket, endpointUrl));

            // assertThat(jsonElement(baos.toString())).isEqualTo(jsonElement);

        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken
        //###TODO aws queue receiver "limit" is broken

  }

  // @Test
  // public void stressTest() throws Exception {
  //   final int limit = 20;

  //   // send
  //   for (int i = 0; i < limit; ++i) {
  //     SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody("{}").build();
  //     SendMessageResponse sendMessageResponse = client.sendMessage(sendMessageRequest);
  //     debug("stressTest", i, sendMessageRequest, sendMessageResponse);
  //   }

  //   // receive
  //   String sourceArg = String.format("%s,endpoint=%s,limit=%s", queueArn, endpointUrl, limit);
  //   Main.main(sourceArg);
  // }

  private JsonElement jsonElement(String json) {
    return new JsonStreamParser(json).next();
  }

  private void log(Object... args) {
    System.out.println(getClass().getSimpleName()+Lists.newArrayList(args));
  }

}
