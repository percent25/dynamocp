package awscat.plugins;

/**
 * base options common for aws plugins
 */
public class BaseOptions {

  /**
   * @see aws-cli --endpoint-url
   * @see aws-sdk-2-java endpointOverride
   */
  public String endpoint;

  /**
   * @see AWS_PROFILE
   * @see aws-cli --profile
   */
  public String profile;
}
