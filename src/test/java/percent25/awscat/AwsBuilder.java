package percent25.awscat;

import java.net.URI;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.gson.reflect.TypeToken;

import percent25.awscat.Addresses;

import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.StringUtils;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;

public class AwsBuilder {
  /**
   * build
   * 
   * @param <B>
   * @param <C>
   * @param builder
   * @return
   */
  public static <B extends AwsClientBuilder<B, C>, C> C build(B builder) {
    String awsEndpoint = new StandardEnvironment().getProperty("aws.endpoint");
    if (StringUtils.hasText(awsEndpoint)) {
      builder = builder.endpointOverride(URI.create(awsEndpoint));
      // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
      builder = builder.region(Region.US_EAST_1);
      builder = builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
    }
    return builder.build();
  }
}
