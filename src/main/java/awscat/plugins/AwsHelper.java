package awscat.plugins;

import java.net.*;

import com.google.gson.*;

import org.springframework.util.*;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.awscore.client.builder.*;

public class AwsHelper {

  private class AwsOptions {
    public String endpoint;
    public String profile;
  }

  /**
   * options
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
  //###TODO RENAME ME TO CONFIGCLIENT (VS OPTIONS)

  //###TODO RENAME ME TO CONFIGCLIENT (VS OPTIONS)

  //###TODO RENAME ME TO CONFIGCLIENT (VS OPTIONS)
  public static <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT> //
  AwsClientBuilder<BuilderT, ClientT> options(AwsClientBuilder<BuilderT, ClientT> builder, Object options) {
    AwsOptions awsOptions = new Gson().fromJson(new Gson().toJson(options), AwsOptions.class);
    if (StringUtils.hasText(awsOptions.endpoint)) {
      builder.endpointOverride(URI.create(awsOptions.endpoint)).build();
    }
    if (StringUtils.hasText(awsOptions.profile)) {
      builder.credentialsProvider(ProfileCredentialsProvider.create(awsOptions.profile));
    }
    return builder;
  }

}
