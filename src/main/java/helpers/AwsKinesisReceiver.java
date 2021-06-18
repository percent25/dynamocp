package helpers;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
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

  // https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-sdk.html
  public ListenableFuture<?> start() throws Exception {
    debug("start", streamName);
    return new FutureRunner() {
      {
        run(() -> {
          return lf(client.listShards(ListShardsRequest.builder().streamName(streamName).build()));
        }, listShardsResponse -> {
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
        });
      }

      class GetRecordsWork {
        public boolean success;
        public String failureMessage;
        public final String streamName;
        public int in;
        public int out;
        public int err;
        public GetRecordsWork(String streamName) {
          this.streamName = streamName;
        }
        public String toString() {
          return new Gson().toJson(this);
        }
      }
      
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
                  ++getRecordsWork.in;
                  ListenableFuture<?> lf = listener.apply(record.data());
                  futures.add(lf);
                  return lf;
                }, lf->{
                  ++getRecordsWork.out;
                }, e->{
                  ++getRecordsWork.err;
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
                } catch (InterruptedException e) {
                  e.printStackTrace();
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
    // System.out.println(Lists.newArrayList(args));
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
    Thread.sleep(20000);
    lf.cancel(true); // stop receiver
    lf.get(); // wait for receiver

  }

}