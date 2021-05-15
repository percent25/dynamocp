package helpers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

import com.google.common.base.*;
import com.google.common.util.concurrent.*;
import com.google.gson.Gson;

import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;

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

// At-most-once aws sqs message receiver
public class AwsQueueMessageReceiver {

  private final SqsAsyncClient sqsClient;
  private final String queueArnOrUrl;
  private final int concurrency;

  private boolean running;
  private Function<String, ListenableFuture<?>> listener;

  // private final List<ListenableFuture<?>> futures = Collections.synchronizedList(new ArrayList<>());

  //###TODO USE HashedWheelTimer
  //###TODO USE HashedWheelTimer
  //###TODO USE HashedWheelTimer
  private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build()); // for backoff
  //###TODO USE HashedWheelTimer
  //###TODO USE HashedWheelTimer
  //###TODO USE HashedWheelTimer

  private final String queueUrl;

  /**
   * ctor
   * 
   * @param queueArnOrUrl
   */
  public AwsQueueMessageReceiver(SqsAsyncClient sqsClient, String queueArnOrUrl, int concurrency) throws Exception {
    debug("ctor", queueArnOrUrl, concurrency);
    this.sqsClient = sqsClient;
    this.queueArnOrUrl = queueArnOrUrl;
    this.concurrency = concurrency;
    this.queueUrl = getQueueUrl(queueArnOrUrl);
  }

  private String getQueueUrl(String queueArnOrUrl) throws Exception {
    // is it a queue arn? e.g., arn:aws:sqs:us-east-1:000000000000:MyQueue
    if (queueArnOrUrl.matches("arn:(.+):sqs:(.+):(\\d{12}):(.+)")) {
      // yes
      String queueName = queueArnOrUrl.substring(queueArnOrUrl.lastIndexOf(':') + 1);
      return sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).get().queueUrl();
    }
    return queueArnOrUrl;
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
    return MoreObjects.toStringHelper(this).add("queueArnOrUrl", queueArnOrUrl).add("c", concurrency).toString();
  }

  /**
   * start
   */
  public void start() {
    debug("start", queueArnOrUrl, concurrency);
    running = true;
    for (int i = 0; i < concurrency; ++i)
      doReceiveMessage(i);
  }

  /**
   * close
   */
  public void close() {
    debug("close", queueArnOrUrl, concurrency);
    running = false;
    // executorService.shutdownNow();
    // futures.forEach(f->f.cancel(true));
  }

  class MessageConsumedRecord {
    public boolean success;
    public String failureMessage;
    public final String queueArnOrUrl;
    public MessageConsumedRecord(String queueArnOrUrl) {
      this.queueArnOrUrl = queueArnOrUrl;
    }
    public String toString() {
      return new Gson().toJson(this);
    }
  }

  private void doReceiveMessage(int i) {
    if (running) {
      ListenableFuture<?> lf = new FutureRunner(){{
        run(() -> {
          ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
              //
              .queueUrl(queueUrl)
              //
              .waitTimeSeconds(20)
              //
              .build();
          return lf(sqsClient.receiveMessage(receiveMessageRequest));
        }, receiveMessageResponse->{
          if (receiveMessageResponse.hasMessages()) {
            for (Message message : receiveMessageResponse.messages()) {
              run(() -> {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    //
                    .queueUrl(queueUrl).receiptHandle(message.receiptHandle()).build();
                return lf(sqsClient.deleteMessage(deleteMessageRequest));
              }, deleteMessageResponse -> {
                MessageConsumedRecord record = new MessageConsumedRecord(queueArnOrUrl);
                run(()->{
                  String body = message.body();
                  try {
                    AwsNotification notification = new Gson().fromJson(body, AwsNotification.class);
                    if (notification.isNotificationType())
                      body = notification.Message;
                  } catch (Exception e) {
                    // do nothing
                  }
                  return listener.apply(body);
                }, result->{
                  record.success=true;
                }, e->{ // listener
                  record.failureMessage = ""+e;
                }, ()->{
                  debug(record);
                });
              }, e->{ // deleteMessage
                debug(e);
              });
            }
          }
        }, e->{ // receiveMessage
            debug(e);
            run(()->{
              // backoff
              // ###TODO USE HashedWheelTimer
              // ###TODO USE HashedWheelTimer
              // ###TODO USE HashedWheelTimer
              return Futures.scheduleAsync(()->Futures.immediateVoidFuture(), Duration.ofSeconds(25), executorService);
              // ###TODO USE HashedWheelTimer
              // ###TODO USE HashedWheelTimer
              // ###TODO USE HashedWheelTimer
            });
          });
        }}.get();

      // futures.add(lf);
      lf.addListener(()->{
        // futures.remove(lf);
        doReceiveMessage(i);
      }, MoreExecutors.directExecutor());
    }
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}