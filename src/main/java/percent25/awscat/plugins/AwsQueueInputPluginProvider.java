package percent25.awscat.plugins;

import java.util.function.Function;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import org.springframework.stereotype.Service;

import helpers.*;
import percent25.awscat.*;
import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

class AwsQueueInputPlugin implements InputPlugin {

  private final AwsQueueReceiver receiver;
  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  public AwsQueueInputPlugin(AwsQueueReceiver receiver) {
    debug("ctor");
    this.receiver = receiver;
    receiver.setListener(message -> {
      return listener.apply(Lists.newArrayList(new JsonStreamParser(message)));
    });
  }

  public String toString() {
    return MoreObjects.toStringHelper(this).add("receiver", receiver).toString();
  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  @Override
  public ListenableFuture<?> run(int mtuHint) throws Exception {
    debug("run", mtuHint);
    return receiver.start();
  }

  @Override
  public void closeNonBlocking() {
    debug("closeNonBlocking");
    receiver.closeNonBlocking();
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}

class AwsQueueInputPluginOptions extends AwsOptions {
  public int c;
  public String toString() {
    return new Gson().toJson(this);
  }
}

@Service
public class AwsQueueInputPluginProvider extends AbstractPluginProvider implements InputPluginProvider {

  private String queueUrl;
  private AwsQueueInputPluginOptions options;

  public AwsQueueInputPluginProvider() {
    super("sqs:<queueName>", AwsQueueInputPluginOptions.class);
  }

  @Override
  public String toString() {
    // return new Gson().toJson(this);
    return MoreObjects.toStringHelper(this).add("queueUrl", queueUrl).add("options", options).toString();
  }

  // https://docs.aws.amazon.com/general/latest/gr/sqs-service.html
  @Override
  public boolean canActivate(String address) {
    queueUrl = Addresses.base(address);
    options = Addresses.options(address, AwsQueueInputPluginOptions.class);
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
  public InputPlugin activate(String address) throws Exception {
    int c = options.c > 0 ? options.c : Runtime.getRuntime().availableProcessors();
    SqsAsyncClient sqsClient = AwsHelper.build(SqsAsyncClient.builder(), options);

    // is it a queue arn? e.g., arn:aws:sqs:us-east-1:000000000000:MyQueue
    if (queueUrl.matches("arn:(.+):sqs:(.+):(\\d{12}):(.+)")) {
      // yes- get queueName
      queueUrl = String.format("sqs:%s", queueUrl.split(":")[5]);
    }

    // is it a queueName?
    if ("sqs".equals(queueUrl.split(":")[0])) {
      // yes- get queueUrl
      String queueName = queueUrl.split(":")[1];
      GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
      GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(getQueueUrlRequest).get();
      queueUrl = getQueueUrlResponse.queueUrl();
    }

    return new AwsQueueInputPlugin(new AwsQueueReceiver(sqsClient, queueUrl, c));
  }

}
