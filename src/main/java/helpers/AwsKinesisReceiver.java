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

// class DoStreamWork {
//   public final String streamName;
//   public boolean success;
//   public String failureMessage;
//   public DoStreamWork(String streamName) {
//     this.streamName = streamName;
//   }
//   public String toString() {
//     return getClass().getSimpleName()+new Gson().toJson(this);
//   }
// }

class GetRecordsWork { //###TODO RENAME TO HANDLESHARDWORK
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

  private boolean running;
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
   */
  public ListenableFuture<?> start() throws Exception {
    debug("start", streamName);
    running = true;
    return new FutureRunner() {
      {
        run(() -> {
          return lf(client.listShards(ListShardsRequest.builder().streamName(streamName).build()));
        }, listShardsResponse -> {
          if (listShardsResponse.hasShards()) {
            for (Shard shard : listShardsResponse.shards()) {
              run(() -> {
                GetShardIteratorRequest getShardIteratorRequest = GetShardIteratorRequest.builder() //
                    .streamName(streamName) //
                    .shardId(shard.shardId()) //
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON) //
                    .build();
                return lf(client.getShardIterator(getShardIteratorRequest));
              }, getShardIteratorResponse -> {
                doGetRecords(getShardIteratorResponse.shardIterator());
              });
            }
          }
        });
      }

      /**
       * doGetRecords
       * 
       * @param shardIterator
       */
      void doGetRecords(String shardIterator) {
        if (running) {
          GetRecordsWork getRecordsWork = new GetRecordsWork(streamName);
          run(() -> {

            // * Each data record can be up to 1 MiB in size, and each shard can read up to 2 MiB per second. You can ensure that
            // * your calls don't exceed the maximum supported size or throughput by using the <code>Limit</code> parameter to
            // * specify the maximum number of records that <a>GetRecords</a> can return. Consider your average record size when
            // * determining this limit. The maximum number of records that can be returned per call is 10,000.
            // * </p>
            // * <p>
            // * The size of the data returned by <a>GetRecords</a> varies depending on the utilization of the shard. The maximum
            // * size of data that <a>GetRecords</a> can return is 10 MiB. If a call returns this amount of data, subsequent calls
            // * made within the next 5 seconds throw <code>ProvisionedThroughputExceededException</code>. If there is
            // * insufficient provisioned throughput on the stream, subsequent calls made within the next 1 second throw
            // * <code>ProvisionedThroughputExceededException</code>. <a>GetRecords</a> doesn't return any data when it throws an
            // * exception. For this reason, we recommend that you wait 1 second between calls to <a>GetRecords</a>. However, it's
            // * possible that the application will get exceptions for longer than 1 second.
       
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
                  getRecordsWork.failureMessage = "" + e; // this is futile
                });
              }
            }
            run(() -> {
              return Futures.allAsList(futures);
            }, all -> {
              getRecordsWork.success = true;
            }, e->{
              getRecordsWork.failureMessage = "" + e;
            }, ()->{
              // STEP 1 log work
              debug(getRecordsWork);
              // STEP 2 find work to do
              String nextShardIterator = getRecordsResponse.nextShardIterator();
              if (nextShardIterator != null) {
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException ie) {
                  ie.printStackTrace();
                }
                doGetRecords(nextShardIterator);
              }
            });
          });
        }
      }
    };
  }

  /**
   * close
   */
  public void closeNonBlocking() {
    debug("close", streamName);
    //###TODO WAIT FOR GRACEFUL SHUTDOWN HERE
    //###TODO WAIT FOR GRACEFUL SHUTDOWN HERE
    //###TODO WAIT FOR GRACEFUL SHUTDOWN HERE
    running = false;
    //###TODO WAIT FOR GRACEFUL SHUTDOWN HERE
    //###TODO WAIT FOR GRACEFUL SHUTDOWN HERE
    //###TODO WAIT FOR GRACEFUL SHUTDOWN HERE
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

  public static void main(String... args) throws Exception {

    KinesisAsyncClient client = KinesisAsyncClient.builder() //
        // .httpClient(AwsCrtAsyncHttpClient.create()) //
        .endpointOverride(URI.create("http://localhost:4566")) //
        .region(Region.US_EAST_1) //
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
        .build();

    // aws kinesis create-stream --stream-name asdf --shard-count 1
    AwsKinesisReceiver receiver = new AwsKinesisReceiver(client, "asdf");
    receiver.setListener(bytes -> {
      System.out.println(bytes);
      return Futures.immediateVoidFuture();
    });
    ListenableFuture<?> lf = receiver.start();
    try {
      Thread.sleep(10000);
      receiver.closeNonBlocking(); // stop receiver
    } finally {
      lf.get();
    }

  }

}