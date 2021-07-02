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
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

@Service
public class AwsQueueOutputPluginProvider implements OutputPluginProvider {

  class Options extends AwsOptions {
    public String toString() {
      return new Gson().toJson(this);
    }
  }

  private String queueUrl;
  private Options options;

  @Override
  public String help() {
    return "sqs:<queueName>";
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("queueUrl", queueUrl).toString();
  }

  // https://docs.aws.amazon.com/general/latest/gr/sqs-service.html
  @Override
  public boolean canActivate(String address) {
    queueUrl = Addresses.base(address);
    options = Addresses.options(address, Options.class);
    if ("sqs".equals(queueUrl.split(":")[0]))
      return true;
    if (queueUrl.matches("arn:(.+):sqs:(.+):(\\d{12}):(.+)"))
      return true;
    if (queueUrl.matches("https://sqs.(.+).amazonaws.(.+)/(\\d{12})/(.+)")) //###TODO fips
      return true;
    if (queueUrl.matches("https://queue.amazonaws.(.+)/(\\d{12})/(.+)")) // legacy
      return true;
    return false;
  }

  @Override
  public Supplier<OutputPlugin> activate(String address) throws Exception {
    SqsAsyncClient sqsClient = AwsHelper.create(SqsAsyncClient.builder(), options);

    // is it a queue arn? e.g., arn:aws:sqs:us-east-1:000000000000:MyQueue
    if (queueUrl.matches("arn:(.+):sqs:(.+):(\\d{12}):(.+)")) {
      // yes- get queueName
      queueUrl = String.format("sqs:%s", queueUrl.split(":")[5]);
    }

    // is it a queueName?
    if ("sqs".equals(queueUrl.split(":")[0])) {
      // yes - get queueUrl
      String queueName = queueUrl.split(":")[1];
      GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
      GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(getQueueUrlRequest).get();
      queueUrl = getQueueUrlResponse.queueUrl();
    }

    // sqs transport is thread-safe therefore the supplier can re-use it
    ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsQueue(sqsClient, queueUrl);

    // ConcatenatedJsonWriter is not thread-safe
    // which makes ConcatenatedJsonWriterOutputPlugin not thread-safe
    return () -> new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
  }

}