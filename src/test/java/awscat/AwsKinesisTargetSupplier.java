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

public class AwsKinesisTargetSupplier implements Supplier<TargetArg> {

  @Override
  public TargetArg get() {
    return new TargetArg() {

      private String endpointUrl;
      private KinesisClient client;

      private String streamName;

      @Override
      public void setUp() {

        endpointUrl = String.format("http://localhost:%s", System.getProperty("edge.port", "4566"));

        client = KinesisClient.builder() //
            // .httpClient(AwsCrtAsyncHttpClient.create()) //
            .endpointOverride(URI.create(endpointUrl)) //
            .region(Region.US_EAST_1) //
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
            .build();

        streamName = UUID.randomUUID().toString();

        CreateStreamRequest createRequest = CreateStreamRequest.builder().streamName(streamName).shardCount(1).build();
        log(createRequest);
        CreateStreamResponse createResponse = client.createStream(createRequest);
        log(createResponse);

      }

      @Override
      public String targetArg() {
        return String.format("kinesis:%s,endpoint=%s", streamName, endpointUrl);
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
