package main.helpers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

import com.google.common.collect.Lists;
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

  private final String queueUrl;
  private final SqsAsyncClient sqsClient = SqsAsyncClient.create();

  private boolean running;
  private Function<String, ListenableFuture<?>> listener;

  private final List<ListenableFuture<?>> futures = Lists.newCopyOnWriteArrayList();

  private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build()); // for backoff

  /**
   * ctor
   * 
   * @param queueUrl
   */
  public AwsQueueMessageReceiver(String queueUrl) {
    log("ctor", queueUrl);
    this.queueUrl = queueUrl;
  }

  /**
   * setListener
   * 
   * @param listener
   */
  public void setListener(Function<String, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  /**
   * start
   */
  public void start() {
    log("start", queueUrl);
    running = true;
    for (int i = 0; i < 4; ++i)
      doReceiveMessage(i);
  }

  /**
   * close
   */
  public void close() {
    log("close", queueUrl);
    running = false;
    // executorService.shutdownNow();
    // futures.forEach(f->f.cancel(true));
  }

  class MessageConsumedRecord {
    public boolean success;
    public String failureMessage;
    public final String queueUrl;
    public MessageConsumedRecord(String queueUrl) {
      this.queueUrl = queueUrl;
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
                MessageConsumedRecord record = new MessageConsumedRecord(queueUrl);
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
                  log(record);
                });
              }, e->{ // deleteMessage
                log(e);
              });
            }
          }
        }, e->{ // receiveMessage
          log(e);
          run(()->{
            // backoff
            return Futures.scheduleAsync(()->Futures.immediateVoidFuture(), Duration.ofSeconds(25), executorService);
          });
        });
      }}.get();

      futures.add(lf);
      lf.addListener(()->{
        futures.remove(lf);
        doReceiveMessage(i);
      }, MoreExecutors.directExecutor());
    }
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}