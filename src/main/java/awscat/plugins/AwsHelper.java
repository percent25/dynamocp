package awscat.plugins;

import java.net.*;

import com.google.gson.*;

import org.springframework.util.*;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.awscore.client.builder.*;
import software.amazon.awssdk.regions.*;

class AwsOptions {
  public String endpoint;
  // public String region;
  public String profile;
}

public class AwsHelper {

  /**
   * configClient
   * 
   * <p>
   * configure aws endpoint and aws profile
   * 
   * @param <BuilderT>
   * @param <ClientT>
   * @param builder
   * @param options
   * @return
   */
  public static <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT> //
  AwsClientBuilder<BuilderT, ClientT> configClient(AwsClientBuilder<BuilderT, ClientT> builder, Object options) {
    AwsOptions awsOptions = new Gson().fromJson(new Gson().toJson(options), AwsOptions.class);
    if (StringUtils.hasText(awsOptions.endpoint)) {
      builder = builder.endpointOverride(URI.create(awsOptions.endpoint));
      //###TODO ugly
      //###TODO ugly
      //###TODO ugly
      builder = builder.region(Region.US_EAST_1); // ### TODO for localstack
      builder = builder.credentialsProvider(AnonymousCredentialsProvider.create()); // ### TODO for localstack
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
    return builder;
  }

}
