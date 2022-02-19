package helpers;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

class AwsNotification {
  public String Type;
  public String MessageId;
  public String TopicArn;
  public String Message;
  public String Timestamp;

  public boolean isNotificationType() {
    return "Notification".equals(Type);
  }

  public String toString() {
    return new Gson().toJson(this);
  }
}

class ReceiveMessageWork {
  public final String queueUrl;
  public boolean success;
  public String failureMessage; // iff !success
  public final AtomicInteger in = new AtomicInteger();
  public final AtomicInteger inErr = new AtomicInteger();
  public final AtomicInteger out = new AtomicInteger();
  public final AtomicInteger outErr = new AtomicInteger();
  public ReceiveMessageWork(String queueUrl) {
    this.queueUrl = queueUrl;
  }
  public String toString() {
    return getClass().getSimpleName()+new Gson().toJson(this);
  }
}

// At-most-once aws sqs message receiver
public class AwsQueueReceiver {

  private final SqsAsyncClient sqsClient;
  private final String queueUrl;
  private final int concurrency;

  private boolean running;
  private Function<String, ListenableFuture<?>> listener;

  /**
   * ctor
   * 
   * @param queueUrl
   */
  public AwsQueueReceiver(SqsAsyncClient sqsClient, String queueUrl, int concurrency) throws Exception {
    debug("ctor", queueUrl, concurrency);
    this.sqsClient = sqsClient;
    this.queueUrl = queueUrl;
    this.concurrency = concurrency;
  }

  /**
   * setListener
   * 
   * @param listener
   */
  public void setListener(Function<String, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  public String toString() {
    return MoreObjects.toStringHelper(this).add("queueUrl", queueUrl).add("c", concurrency).toString();
  }

  /**
   * start
   */
  public ListenableFuture<?> start() {
    debug("start", queueUrl, concurrency);
    running = true;
    return new FutureRunner() {
      {
        for (int i = 0; i < concurrency; ++i)
          doReceiveMessage(i);
      }
      void doReceiveMessage(int i) {
        if (running) {
          ReceiveMessageWork work = new ReceiveMessageWork(queueUrl);
          run(()->{
            return new FutureRunner() {
              {
                run(() -> {
                  ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder() //
                      .queueUrl(queueUrl) //
                      .maxNumberOfMessages(10) //
                      .waitTimeSeconds(20) //
                      .build();
                  return lf(sqsClient.receiveMessage(receiveMessageRequest));
                }, receiveMessageResponse -> {
                  run(()->{
                    return new FutureRunner(){{
                      if (receiveMessageResponse.hasMessages()) {
                        for (Message message : receiveMessageResponse.messages()) {
                          run(() -> {
                            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder() //
                                .queueUrl(queueUrl) //
                                .receiptHandle(message.receiptHandle()) //
                                .build();
                            return lf(sqsClient.deleteMessage(deleteMessageRequest));
                          }, deleteMessageResponse -> { // deleteMessageSuccess
                            work.in.incrementAndGet();
                            run(() -> {
                              String body = message.body();
                              try {
                                // is the string message boxed in a aws notification?
                                AwsNotification notification = new Gson().fromJson(body, AwsNotification.class);
                                if (notification.isNotificationType())
                                  body = notification.Message; // yes.. unbox the string message
                              } catch (Exception e) {
                                // do nothing
                              }
                              return listener.apply(body);
                            }, result -> {
                              work.out.incrementAndGet();
                            }, e -> { // listener
                              debug(e);
                              work.outErr.incrementAndGet();
                              work.failureMessage = "" + e;
                            });
                          }, e -> { // deleteMessageFailure
                            debug(e);
                            work.inErr.incrementAndGet();
                            work.failureMessage = "deleteMessage:" + e;
                          });
                        }
                      }
                    }}.get();
                  }, result->{
                    work.success = true;
                  }, e->{
                    debug(e);
                    work.failureMessage = "" + e;
                  });
                }, e -> { // receiveMessageFailure
                  debug(e);
                  work.failureMessage = "receiveMessage:" + e;
                  if (running) {
                    // backoff/retry
                    run(()->{
                      return sleep(20_000);
                    });
                  }
                });
              }
            }.get();
          }, ()->{ // finally
            debug("work", work);
            doReceiveMessage(i);
          });
        } // isRunning
      }
    }.get();
  }

  /**
   * close
   */
  public void closeNonBlocking() {
    running = false;
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

  public static void main(String... args) throws Exception {

    LogHelper.debug = true;

    SqsAsyncClient client = SqsAsyncClient.builder() //
        // .httpClient(AwsCrtAsyncHttpClient.create()) //
        .endpointOverride(URI.create("http://localhost:4566")) //
        .region(Region.US_EAST_1) //
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
        .build();

    AwsQueueReceiver receiver = new AwsQueueReceiver(client, "http://localhost:4566/000000000000/MyQueue", 1);

    receiver.setListener(s -> {
      System.out.println(s);
      // receiver.closeNonBlocking();
      return Futures.immediateVoidFuture();
    });

    System.out.println("start");
    ListenableFuture<?> lf = receiver.start();
    // System.out.println("sleep");
    // Thread.sleep(2000);
    // System.out.println("close");
    // receiver.closeNonBlocking(); // stop receiver
    lf.get();

  }

}