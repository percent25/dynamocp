package awscat;

import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.*;
import software.amazon.awssdk.services.kinesis.model.*;

public class AwsKinesisTargetSupplier implements Supplier<OutputTargetArg> {

  @Override
  public OutputTargetArg get() {
    return new OutputTargetArg() {

      private KinesisClient client;

      private String streamName;

      @Override
      public void setUp() {

        client = AwsBuilder.build(KinesisClient.builder());

        streamName = UUID.randomUUID().toString();

        CreateStreamRequest createRequest = CreateStreamRequest.builder().streamName(streamName).shardCount(1).build();
        log(createRequest);
        CreateStreamResponse createResponse = client.createStream(createRequest);
        log(createResponse);

        client.waiter().waitUntilStreamExists(a->a.streamName(streamName));
      }

      @Override
      public String address() {
        return String.format("kinesis:%s", streamName);
      }

      @Override
      public JsonElement verify() {
        ListShardsRequest listShardsRequest = ListShardsRequest.builder().streamName(streamName).build();
        log(listShardsRequest);
        ListShardsResponse listShardsResponse = client.listShards(listShardsRequest);
        log(listShardsResponse);

        Shard shard = listShardsResponse.shards().iterator().next();

        GetShardIteratorRequest getShardIteratorRequest = GetShardIteratorRequest.builder() //
            .streamName(streamName) //
            .shardId(shard.shardId()) //
            .shardIteratorType(ShardIteratorType.TRIM_HORIZON) //
            .build();
        log(getShardIteratorRequest);
        GetShardIteratorResponse getShardIteratorResponse = client.getShardIterator(getShardIteratorRequest);
        log(getShardIteratorResponse);

        String shardIterator = getShardIteratorResponse.shardIterator();

        GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder() //
            .shardIterator(shardIterator) //
            .build();
        log(getRecordsRequest);
        GetRecordsResponse getRecordsResponse = client.getRecords(getRecordsRequest);
        log(getRecordsResponse);

        //###TODO may need to iterator over nextShardIterator here
        //###TODO may need to iterator over nextShardIterator here
        //###TODO may need to iterator over nextShardIterator here
        return json(getRecordsResponse.records().iterator().next().data().asUtf8String());
        //###TODO may need to iterator over nextShardIterator here
        //###TODO may need to iterator over nextShardIterator here
        //###TODO may need to iterator over nextShardIterator here
      }

      @Override
      public void tearDown() {
        DeleteStreamRequest deleteRequest = DeleteStreamRequest.builder().streamName(streamName).build();
        log(deleteRequest);
        DeleteStreamResponse deleteResponse = client.deleteStream(deleteRequest);
        log(deleteResponse);
      }

      private JsonElement json(String json) {
        return new JsonStreamParser(json).next();
      }

      private void log(Object arg) {
        System.out.println(getClass().getSimpleName() + arg);
      }
    };
  }

}
