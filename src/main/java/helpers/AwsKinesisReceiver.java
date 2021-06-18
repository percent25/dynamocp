package helpers;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.base.*;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import com.google.gson.Gson;

import awscat.plugins.AwsHelper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

class ListShardsWork {
  public final String streamName;
  public boolean success;
  public String failureMessage;
  public ListShardsWork(String streamName) {
    this.streamName = streamName;
  }
  public String toString() {
    return getClass().getSimpleName()+new Gson().toJson(this);
  }
}

class GetRecordsWork {
  public final String streamName;
  public boolean success;
  public String failureMessage;
  public final AtomicInteger in = new AtomicInteger(); // aka request
  public final AtomicInteger out = new AtomicInteger(); // aka success
  public final AtomicInteger err = new AtomicInteger(); // aka failure
  public GetRecordsWork(String streamName) {
    this.streamName = streamName;
  }
  public String toString() {
    return getClass().getSimpleName()+new Gson().toJson(this);
  }
}

// at-most-once aws kinesis receiver
public class AwsKinesisReceiver {

  private final KinesisAsyncClient client;
  private final String streamName;

  private Function<SdkBytes, ListenableFuture<?>> listener;

  /**
   * ctor
   * 
   * @param streamName
   */
  public AwsKinesisReceiver(KinesisAsyncClient client, String streamName) throws Exception {
    debug("ctor", streamName);
    this.client = client;
    this.streamName = streamName;
  }

  /**
   * setListener
   * 
   * @param listener
   */
  public void setListener(Function<SdkBytes, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  public String toString() {
    return MoreObjects.toStringHelper(this).add("streamName", streamName).toString();
  }
  
  /**
   * start
   * 
   * @see https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-sdk.html
   * 
   * @return
   * @throws Exception
   */
  public ListenableFuture<?> start() throws Exception {
    debug("start", streamName);
    return new FutureRunner() {

      {
        doListShards();
      }

      /**
       * doListShards
       */
      void doListShards() {
        if (isRunning()) {
          ListShardsWork listShardsWork = new ListShardsWork(streamName);
          run(() -> {
            return lf(client.listShards(ListShardsRequest.builder().streamName(streamName).build()));
          }, listShardsResponse -> {
            listShardsWork.success = true;
            if (listShardsResponse.hasShards()) {
              for (Shard shard : listShardsResponse.shards()) {
                run(() -> {
                  GetShardIteratorRequest getShardIteratorRequest = GetShardIteratorRequest.builder() //
                      .streamName(streamName) //
                      .shardId(shard.shardId()) //
                      .shardIteratorType(ShardIteratorType.LATEST) //
                      .build();
                  return lf(client.getShardIterator(getShardIteratorRequest));
                }, getShardIteratorResponse -> {
                  doGetRecords(getShardIteratorResponse.shardIterator());
                });
              }
            }
          }, e->{
            listShardsWork.failureMessage = ""+e;
            // backoff/retry
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              ie.printStackTrace();
            }
            doListShards();
          }, ()->{
            debug(listShardsWork);
          });
        }
      }

      /**
       * doGetRecords
       * 
       * @param shardIterator
       */
      void doGetRecords(String shardIterator) {
        if (isRunning()) {
          GetRecordsWork getRecordsWork = new GetRecordsWork(streamName);
          run(() -> {
            return lf(client.getRecords(GetRecordsRequest.builder().shardIterator(shardIterator).build()));
          }, getRecordsResponse -> {
            List<ListenableFuture<?>> futures = new ArrayList<>();
            if (getRecordsResponse.hasRecords()) {
              for (Record record : getRecordsResponse.records()) {
                run(() -> {
                  getRecordsWork.in.incrementAndGet();
                  ListenableFuture<?> lf = listener.apply(record.data());
                  futures.add(lf);
                  return lf;
                }, lf->{
                  getRecordsWork.out.incrementAndGet();
                }, e->{
                  getRecordsWork.err.incrementAndGet();
                  getRecordsWork.failureMessage = ""+e;
                });
              }
            }
            run(() -> {
              return Futures.allAsList(futures);
            }, all -> {
              getRecordsWork.success = true;
              String nextShardIterator = getRecordsResponse.nextShardIterator();
              if (nextShardIterator != null) {
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException ie) {
                  ie.printStackTrace();
                }
                doGetRecords(nextShardIterator);
              }
            }, e->{
              getRecordsWork.failureMessage = "" + e;
            }, ()->{
              debug(getRecordsWork);
            });
          });
        }
      }
    };
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
    System.out.println(Lists.newArrayList(args));
  }

  public static void main(String... args) throws Exception {

    KinesisAsyncClient client = KinesisAsyncClient.builder() //
        // .httpClient(AwsCrtAsyncHttpClient.create()) //
        .endpointOverride(URI.create("http://localhost:4566")) //
        .region(Region.US_EAST_1) //
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
        .build();

    AwsKinesisReceiver receiver = new AwsKinesisReceiver(client, "MyStream");
    receiver.setListener(bytes -> {
      System.out.println(bytes);
      return Futures.immediateVoidFuture();
    });
    ListenableFuture<?> lf = receiver.start();
    Thread.sleep(10000);
    lf.cancel(true); // stop receiver
    lf.get(); // wait for receiver

  }

}