package awscat;

import java.net.*;

import org.junit.jupiter.api.*;

import software.amazon.awssdk.services.s3.*;

public class BasicSmokeIT {

  @Test
  public void basicSmokeIT() throws Exception {

    S3Client client = S3Client.builder()
    //
    .endpointOverride(URI.create("http://localhost:4566"))
    //
    .build();

    System.out.println(client.listBuckets());



  }
  
}
