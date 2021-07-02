package awscat.plugins;

import java.net.*;

import com.google.gson.*;

import org.springframework.util.*;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.awscore.client.builder.*;
import software.amazon.awssdk.regions.*;

class AwsOptions {
  public String endpoint;
  // public String region; //###TODO
  // public String access; //###TODO
  // public String secret; //###TODO
  public String profile;
}

public class AwsHelper {

  /**
   * create
   * 
   * <p>
   * configure aws endpoint and aws profile
   * 
   * @param <B>
   * @param <C>
   * @param builder
   * @param options
   * @return
   */
  public static <B extends AwsClientBuilder<B, C> & AwsAsyncClientBuilder<B, C>, C> C create(B builder, Object options) {
    // builder = builder.httpClient(AwsCrtAsyncHttpClient.create());
    AwsOptions awsOptions = new Gson().fromJson(new Gson().toJson(options), AwsOptions.class);
    if (StringUtils.hasText(awsOptions.endpoint)) {
      builder = builder.endpointOverride(URI.create(awsOptions.endpoint));
      //###TODO ugly
      //###TODO ugly
      //###TODO ugly
      // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
      builder = builder.region(Region.US_EAST_1); // ### TODO for localstack
      builder = builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))); // ### TODO for localstack
      //###TODO ugly
      //###TODO ugly
      //###TODO ugly
    }
    // if (StringUtils.hasText(awsOptions.region)) {
    //   builder = builder.region(Region.US_EAST_1);
    // }
    if (StringUtils.hasText(awsOptions.profile)) {
      builder = builder.credentialsProvider(ProfileCredentialsProvider.create(awsOptions.profile));
    }
    return builder.build();
  }

}
