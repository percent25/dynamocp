package awscat.plugins;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import awscat.*;

import org.springframework.stereotype.Service;

import helpers.*;
import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

class AwsQueueInputPlugin implements InputPlugin {

  private final AwsQueueReceiver receiver;
  // private final int limit;

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  private ListenableFuture<?> lf;
  private final AtomicInteger count = new AtomicInteger();

  public AwsQueueInputPlugin(AwsQueueReceiver receiver, int limit) {
    debug("ctor");
    this.receiver = receiver;
    // this.limit = limit;
    receiver.setListener(message -> {
      return new FutureRunner() {
        List<JsonElement> list;
        {
          run(() -> {
            count.updateAndGet(count -> {
              list = Lists.newArrayList(Iterators.limit(new JsonStreamParser(message), limit - count));
              return count + list.size();
            });
            return listener.apply(list);
          }, () -> {
            if (count.get() == limit)
              lf.cancel(true);
          });
        }
      };
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
  public ListenableFuture<?> read(int mtu) throws Exception {
    return lf = receiver.start();
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}

class AwsQueueInputPluginOptions extends AwsOptions {
  public int c;
  public int limit;

  public String toString() {
    return new Gson().toJson(this);
  }
}

@Service
public class AwsQueueInputPluginProvider extends AbstractInputPluginProvider {

  private String queueArnOrUrl;
  private AwsQueueInputPluginOptions options;

  public AwsQueueInputPluginProvider() {
    super("<queue-arn|queue-url>", AwsQueueInputPluginOptions.class);
  }

  @Override
  public String toString() {
    // return new Gson().toJson(this);
    return MoreObjects.toStringHelper(this).add("queueArnOrUrl", queueArnOrUrl).add("options", options).toString();
  }

  // https://docs.aws.amazon.com/general/latest/gr/sqs-service.html
  @Override
  public boolean canActivate(String arg) {
    queueArnOrUrl = Args.base(arg);
    options = Args.options(arg, AwsQueueInputPluginOptions.class);
    if (queueArnOrUrl.matches("arn:(.+):sqs:(.+):(\\d{12}):(.+)"))
      return true;
    if (queueArnOrUrl.matches("https://sqs.(.+).amazonaws.(.*)/(\\d{12})/(.+)")) //###TODO fips
      return true;
    if (queueArnOrUrl.matches("https://queue.amazonaws.(.*)/(\\d{12})/(.+)")) // legacy
      return true;
    return false;
  }

  @Override
  public InputPlugin activate(String arg) throws Exception {
    int c = options.c > 0 ? options.c : Runtime.getRuntime().availableProcessors();
    SqsAsyncClient sqsClient = AwsHelper.configClient(SqsAsyncClient.builder(), options).build();

    String queueUrl = queueArnOrUrl;
    // is it a queue arn? e.g., arn:aws:sqs:us-east-1:000000000000:MyQueue
    if (queueArnOrUrl.matches("arn:(.+):sqs:(.+):(\\d{12}):(.+)")) {
        // yes
        String queueName = queueArnOrUrl.split(":")[5];
        queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).get().queueUrl();
    }

    return new AwsQueueInputPlugin(new AwsQueueReceiver(sqsClient, queueUrl, c), options.limit);
  }

}
