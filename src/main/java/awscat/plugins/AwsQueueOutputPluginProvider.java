package awscat.plugins;

import java.util.function.Supplier;

import com.google.common.base.MoreObjects;
import com.google.gson.Gson;

import org.springframework.stereotype.Service;

import awscat.Addresses;
import awscat.OutputPlugin;
import awscat.OutputPluginProvider;
import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsQueue;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

@Service
public class AwsQueueOutputPluginProvider implements OutputPluginProvider {

  class Options extends AwsOptions {
    public String toString() {
      return new Gson().toJson(this);
    }
  }

  private String queueArnOrUrl;
  private Options options;

  @Override
  public String help() {
    return "<queue-arn|queue-url>";
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("queueArnOrUrl", queueArnOrUrl).toString();
  }

  // https://docs.aws.amazon.com/general/latest/gr/sqs-service.html
  @Override
  public boolean canActivate(String arg) {
    queueArnOrUrl = Addresses.base(arg);
    options = Addresses.options(arg, Options.class);
    if (queueArnOrUrl.matches("arn:(.+):sqs:(.+):(\\d{12}):(.+)"))
      return true;
    if (queueArnOrUrl.matches("https://sqs.(.+).amazonaws.(.*)/(\\d{12})/(.+)")) //###TODO fips
      return true;
    if (queueArnOrUrl.matches("https://queue.amazonaws.(.*)/(\\d{12})/(.+)")) // legacy
      return true;
    return false;
  }

  @Override
  public Supplier<OutputPlugin> activate(String arg) throws Exception {
    SqsAsyncClient sqsClient = AwsHelper.create(SqsAsyncClient.builder(), options);

    String queueUrl = queueArnOrUrl;
    // is it a queue arn? e.g., arn:aws:sqs:us-east-1:000000000000:MyQueue
    if (queueArnOrUrl.matches("arn:(.+):sqs:(.+):(\\d{12}):(.+)")) {
      // yes
      String queueName = queueArnOrUrl.split(":")[5];
      queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).get().queueUrl();
    }

    // sqs transport is thread-safe
    ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsQueue(sqsClient, queueUrl);

    // ConcatenatedJsonWriter is not thread-safe
    // which makes ConcatenatedJsonWriterOutputPlugin not thread-safe
    return () -> new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
  }

}