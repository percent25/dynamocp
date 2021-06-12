package awscat;

import java.net.*;

import org.junit.jupiter.api.*;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.*;
import software.amazon.awssdk.services.s3.*;

public class BasicSmokeIT {

  @Test
  public void basicSmokeIT() throws Exception {

    S3Client client = S3Client.builder() //
        .endpointOverride(URI.create("http://localhost:4566")) //
        .region(Region.US_EAST_1) //
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
        .build();

    System.out.println(client.listBuckets());

  }

}
