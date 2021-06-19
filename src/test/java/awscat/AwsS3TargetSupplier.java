package awscat;

import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AwsS3TargetSupplier implements Supplier<TargetArg> {

  @Override
  public TargetArg get() {
    return new TargetArg() {

      // beforeAll
      String endpointUrl;
      S3Client client;

      // beforeEach
      String bucket;

      @Override
      public void setUp() {

        endpointUrl = String.format("http://localhost:%s", System.getProperty("edge.port", "4566"));

        client = S3Client.builder() //
            // .httpClient(AwsCrtAsyncHttpClient.create()) //
            .endpointOverride(URI.create(endpointUrl)) //
            .region(Region.US_EAST_1) //
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
            .build();

        bucket = UUID.randomUUID().toString();

        CreateBucketRequest request = CreateBucketRequest.builder().bucket(bucket).build();
        log(request);
        CreateBucketResponse response = client.createBucket(request);
        log(response);

      }

      @Override
      public String targetArg() {
        return String.format("s3://%s,endpoint=%s", bucket, endpointUrl);
      }

      @Override
      public JsonElement verify() {
        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucket).build();
        ListObjectsV2Response response = client.listObjectsV2(request);

        String key = response.contents().iterator().next().key();

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
        ResponseBytes<GetObjectResponse> getObjectResponse = client.getObjectAsBytes(getObjectRequest);
        return json(getObjectResponse.asUtf8String());
      }

      @Override
      public void tearDown() {
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

      private JsonElement json(String json) {
        return new JsonStreamParser(json).next();
      }

      private void log(Object arg) {
        System.out.println(arg);
      }
    };
  }

  public static void main(String... args) throws Exception {
    TargetArg target = new AwsS3TargetSupplier().get();
    target.setUp();
    System.out.println(target.targetArg());
    target.tearDown();
  }

}
