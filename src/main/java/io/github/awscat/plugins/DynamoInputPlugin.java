package io.github.awscat.plugins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import helpers.FutureRunner;
import helpers.LogHelper;
import io.github.awscat.Args;
import io.github.awscat.InputPlugin;
import io.github.awscat.InputPluginProvider;
import io.github.awscat.Options;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html
public class DynamoInputPlugin implements InputPlugin {

  private final DynamoDbAsyncClient client;
  private final String tableName;
  private final Iterable<String> keySchema;
  private final DynamoOptions options;
  private final RateLimiter readLimiter;

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList();

  /**
   * ctor
   * 
   * @param client
   * @param tableName
   * @param keySchema
   * @param options
   * @param readLimiter
   */
  public DynamoInputPlugin(DynamoDbAsyncClient client, String tableName, Iterable<String> keySchema, DynamoOptions options, RateLimiter readLimiter) {
    debug("ctor", this);

    this.client = client;
    this.tableName = tableName;
    this.keySchema = keySchema;
    this.options = options;  
    this.readLimiter = readLimiter;

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    exclusiveStartKeys.addAll(Collections.nCopies(options.totalSegments(), null));

  }

  public String toString() {
    // return tableName + keySchema + options + readLimiter;
    return MoreObjects.toStringHelper(this)
        //
        .add("tableName", tableName)
        //
        .add("keySchema", keySchema)
        //
        .add("options", options)
        //
        // .add("readLimiter", readLimiter)
        //
        .toString();
  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  @Override
  public ListenableFuture<?> read(int mtu) throws Exception {
    return new FutureRunner() {
      {
        for (int segment = 0; segment < options.totalSegments(); ++segment)
          doSegment(segment);
      }
      void doSegment(int segment) {
        
        debug("doSegment", segment);

        run(() -> {
  
          // pre-throttle
          readLimiter.acquire(128);

          // STEP 1 Do the scan
          ScanRequest scanRequest = ScanRequest.builder()
              //
              .tableName(tableName)
              //
              .exclusiveStartKey(exclusiveStartKeys.get(segment))
              //
              // .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
              //
              .segment(segment)
              //
              .totalSegments(options.totalSegments())
              //
              // .limit(256)
              //
              .build();

          if (options.limit > 0)
            scanRequest = scanRequest.toBuilder().limit(options.limit).build();

          debug("doSegment", segment, scanRequest);

          return lf(client.scan(scanRequest));
        }, scanResponse -> {

          debug("doSegment", segment, scanResponse.items().size());

          exclusiveStartKeys.set(segment, scanResponse.lastEvaluatedKey());

          // STEP 2 Process the scan

          // readCount.addAndGet(scanResponse.items().size());

          List<JsonElement> jsonElements = new ArrayList<>();

          for (Map<String, AttributeValue> item : scanResponse.items())
            jsonElements.add(parse(item));

          run(()->{
            return listener.apply(jsonElements);
          }, ()->{ // finally
            if (!exclusiveStartKeys.get(segment).isEmpty()) //###TODO check for null here?
              doSegment(segment);
          });

        }, e->{
          debug("doSegment", segment, e);
          e.printStackTrace();
          throw new RuntimeException(e);
        });

      }
    }.get();
  }

  private final ObjectMapper objectMapper = new ObjectMapper();

  private JsonElement parse(Map<String, AttributeValue> item) {
    
    // aesthetics
    var sortedItem = new LinkedHashMap<String, AttributeValue>();
    for (var key : keySchema)
      sortedItem.put(key, item.get(key));
    if (!options.keys)
      sortedItem.putAll(ImmutableSortedMap.copyOf(item));
    
    var jsonElement = new Gson().toJsonTree(Maps.transformValues(sortedItem, value -> {
      try {
        return new Gson().fromJson(objectMapper.writeValueAsString(value.toBuilder()), JsonElement.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }));

        // // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataExport.Output.html
        // var jsonLine = new JsonObject();
        // jsonLine.add("Item", jsonElement);
        // return jsonLine;

    return jsonElement;
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}

@Service
class DynamoInputPluginProvider implements InputPluginProvider {

  private final ApplicationArguments args;
  private final DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().build();

  public DynamoInputPluginProvider(ApplicationArguments args) {
    this.args = args;
  }

  @Override
  public boolean canActivate() {
    String arg = args.getNonOptionArgs().get(0);
    return ImmutableSet.of("dynamo", "dynamodb").contains(Args.base(arg).split(":")[0]);
  }

  @Override
  public InputPlugin get() throws Exception {
    String arg = args.getNonOptionArgs().get(0);
    String tableName = Args.base(arg).split(":")[1];  
    DynamoOptions options = Args.options(arg, DynamoOptions.class);  
    debug("desired", options);

    DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
    DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();

    Iterable<String> keySchema = Lists.transform(describeTableResponse.table().keySchema(), e->e.attributeName());      

    int concurrency = Runtime.getRuntime().availableProcessors();
    int provisionedRcu = describeTableResponse.table().provisionedThroughput().readCapacityUnits().intValue();
    int provisionedWcu = describeTableResponse.table().provisionedThroughput().writeCapacityUnits().intValue();

    options.infer(concurrency, provisionedRcu, provisionedWcu);
    debug("reported", options);

    //###TODO PASS THIS TO DYNAMOINPUTPLUGIN
    //###TODO PASS THIS TO DYNAMOINPUTPLUGIN
    //###TODO PASS THIS TO DYNAMOINPUTPLUGIN
    RateLimiter readLimiter = RateLimiter.create(options.rcu>0?options.rcu:Integer.MAX_VALUE);
    debug("readLimiter", readLimiter);
    //###TODO PASS THIS TO DYNAMOINPUTPLUGIN
    //###TODO PASS THIS TO DYNAMOINPUTPLUGIN
    //###TODO PASS THIS TO DYNAMOINPUTPLUGIN

    return new DynamoInputPlugin(client, tableName, keySchema, options, readLimiter);
  }
  
  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
