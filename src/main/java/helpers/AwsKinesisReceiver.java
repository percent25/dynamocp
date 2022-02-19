package helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

class DoShardIteratorWork {
  public final String streamName;
  public final String shardId;
  public final String shardIterator;
  public boolean success;
  public String failureMessage;
  public final AtomicInteger in = new AtomicInteger(); // aka request
  public final AtomicInteger out = new AtomicInteger(); // aka success
  public final AtomicInteger err = new AtomicInteger(); // aka failure
  public DoShardIteratorWork(String streamName, String shardId, String shardIterator) {
    this.streamName = streamName;
    this.shardId = shardId;
    this.shardIterator = shardIterator;
  }
  public String toString() {
    return new Gson().toJson(this);
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
        doStream();
      }
      
      void doStream() {
        debug("doStream", streamName);
        run(() -> {
          return lf(client.listShards(ListShardsRequest.builder().streamName(streamName).build()));
        }, listShardsResponse -> {
          // if (listShardsResponse.hasShards())
          {
            for (Shard shard : listShardsResponse.shards()) {
              doShard(shard);
            }
          }
        }, e->{
          debug(e);
          try {
            Thread.sleep(1000); // throttle
          } catch (InterruptedException ie) {
          }
          doStream();
        });
      }
      
      void doShard(Shard shard) {
        debug("doShard", streamName, shard.shardId());
        run(() -> {
          GetShardIteratorRequest getShardIteratorRequest = GetShardIteratorRequest.builder() //
              .streamName(streamName) //
              .shardId(shard.shardId()) //
              .shardIteratorType(ShardIteratorType.LATEST) //
              .build();
          return lf(client.getShardIterator(getShardIteratorRequest));
        }, getShardIteratorResponse -> {
          // GetRecordsWork getRecordsWork = new GetRecordsWork(streamName);          
          doShardIterator(shard, getShardIteratorResponse.shardIterator());
        }, e->{
          debug(e);
          try {
            Thread.sleep(1000); // throttle
          } catch (InterruptedException ie) {
          }
          doShard(shard);
        });
      }

      void doShardIterator(Shard shard, String shardIterator) {
        // debug("doShardIterator", streamName, shard.shardId(), shardIterator);
        if (running) {
          DoShardIteratorWork work = new DoShardIteratorWork(streamName, shard.shardId(), shardIterator);
          run(() -> {
            Thread.sleep(1000); // throttle
            return lf(client.getRecords(GetRecordsRequest.builder().shardIterator(shardIterator).build()));
          }, getRecordsResponse -> {
            List<ListenableFuture<?>> futures = new ArrayList<>();
            if (getRecordsResponse.hasRecords()) {
              for (Record record : getRecordsResponse.records()) {
                run(() -> {
                  work.in.incrementAndGet();
                  ListenableFuture<?> lf = listener.apply(record.data());
                  futures.add(lf);
                  return lf;
                }, lf->{
                  work.out.incrementAndGet();
                }, e->{
                  work.err.incrementAndGet();
                  work.failureMessage = "" + e; // this is futile
                });
              }
            }
            run(() -> {
              return Futures.allAsList(futures);
            }, all -> {
              work.success = true;
            }, e->{
              work.failureMessage = "" + e;
            }, ()->{
              if (getRecordsResponse.records().size()>0)
                debug("doShardIterator", work);
              String nextShardIterator = getRecordsResponse.nextShardIterator();
              if (nextShardIterator != null)
                doShardIterator(shard, nextShardIterator);
            });
          }, e->{
            doShard(shard);
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

    // DefaultAwsRegionProviderChain

    LogHelper.debug=true;

    KinesisAsyncClient client = KinesisAsyncClient.builder() //
        // .endpointOverride(URI.create("http://localhost:4566")) //
        // .region(Region.US_EAST_1) //
        // .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
        .region(Region.US_WEST_2)
        .credentialsProvider(ProfileCredentialsProvider.create("cirrascale"))
        .build();

    // aws kinesis create-stream --stream-name asdf --shard-count 1
    AwsKinesisReceiver receiver = new AwsKinesisReceiver(client, "NetBoxWebHookStack-NetBoxWebHookEventStream92261DC8-9LWDNez8q8xM");
    receiver.setListener(bytes -> {
      System.out.println(bytes.asUtf8String());
      // receiver.closeNonBlocking(); // stop receiver
      return Futures.immediateVoidFuture();
    });
    ListenableFuture<?> lf = receiver.start();
    try {
      // Thread.sleep(10000);
      // receiver.closeNonBlocking(); // stop receiver
    } finally {
      System.out.println("finally");
      lf.get();
    }

  }

}