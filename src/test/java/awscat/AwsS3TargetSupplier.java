package awscat;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
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

public class AwsS3TargetSupplier implements Supplier<TargetArg> {

  @Override
  public TargetArg get() {
    return new TargetArg() {

      // beforeAll
      String endpointUrl;
      S3AsyncClient client;

      // beforeEach
      String bucket;

      @Override
      public void setUp() throws Exception {

        endpointUrl = String.format("http://localhost:%s", System.getProperty("edge.port", "4566"));

        client = S3AsyncClient.builder() //
            // .httpClient(AwsCrtAsyncHttpClient.create()) //
            .endpointOverride(URI.create(endpointUrl)) //
            .region(Region.US_EAST_1) //
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
            .build();

        // STEP 1 setup

        bucket = UUID.randomUUID().toString();

        CreateBucketRequest request = CreateBucketRequest.builder().bucket(bucket).build();
        log(request);
        CreateBucketResponse response = client.createBucket(request).get();
        log(response);

      }

      @Override
      public String targetArg() {
        return String.format("s3://%s/asdf,endpoint=%s", bucket, endpointUrl);
      }

      @Override
      public JsonElement verifyAndTearDown() throws Exception {

        try {

          // STEP 1 verify

          ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucket).build();
          ListObjectsV2Response response = client.listObjectsV2(request).get();

          String key = response.contents().iterator().next().key();

          try {

            GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
            ResponseBytes<GetObjectResponse> getObjectResponse = client.getObject(getObjectRequest, AsyncResponseTransformer.toBytes()).get();
  
            return json(getObjectResponse.asInputStream());

          } finally {

            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
            log(deleteObjectRequest);
            DeleteObjectResponse deleteObjectResponse = client.deleteObject(deleteObjectRequest).get();
            log(deleteObjectResponse);

          }

        } finally {

          // STEP 2 teardown

          DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
          log(deleteBucketRequest);
          DeleteBucketResponse deleteBucketResponse = client.deleteBucket(deleteBucketRequest).get();
          log(deleteBucketResponse);
          
        }

      }

      JsonElement json(InputStream in) {
        return new JsonStreamParser(new InputStreamReader(in)).next();
      }

      void log(Object arg) {
        System.out.println(arg);
      }
    };
  }

  public static void main(String... args) throws Exception {

    TargetArg target = new AwsS3TargetSupplier().get();

    target.setUp();
    System.out.println(target.targetArg());
    System.out.println(target.verifyAndTearDown());
    // target.verifyAndTearDown();
  }

}
