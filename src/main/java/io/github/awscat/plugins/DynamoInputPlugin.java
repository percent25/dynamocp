package io.github.awscat.plugins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.springframework.stereotype.Service;

import helpers.FutureRunner;
import helpers.LogHelper;
import io.github.awscat.Args;
import io.github.awscat.InputPlugin;
import io.github.awscat.InputPluginProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html
public class DynamoInputPlugin implements InputPlugin {

  private final DynamoDbAsyncClient client;
  private final String tableName;
  private final Iterable<String> keySchema;
  private final RateLimiter readLimiter;
  private final int totalSegments;
  private final int limit;

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList();

  /**
   * ctor
   * 
   * @param client
   * @param tableName
   * @param keySchema
   * @param readLimiter
   * @param totalSegments
   * @param limit
   */
  public DynamoInputPlugin(DynamoDbAsyncClient client, String tableName, Iterable<String> keySchema, RateLimiter readLimiter, int totalSegments, int limit) {
    debug("ctor");

    this.client = client;
    this.tableName = tableName;
    this.keySchema = keySchema;
    this.readLimiter = readLimiter;
    this.totalSegments = totalSegments;
    this.limit = limit;

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb
    exclusiveStartKeys.addAll(Collections.nCopies(totalSegments, null));
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
        //
        .add("tableName", tableName)
        //
        .add("keySchema", keySchema)
        //
        .add("readLimiter", readLimiter)
        //
        .add("totalSegments", totalSegments)
        //
        // .add("limit", limit)
        //
        .toString();
  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  @Override
  public ListenableFuture<?> read(int mtu) throws Exception {
    debug("read", "mtu", mtu);

    // prewarm limiter
    readLimiter.acquire(Double.valueOf(readLimiter.getRate()).intValue());

    return new FutureRunner() {
      {
        for (int segment = 0; segment < totalSegments; ++segment)
          doSegment(segment);
      }
      void doSegment(int segment) {
        
        debug("doSegment", segment);

        run(() -> {
  
          // pre-throttle
          // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb
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
              .totalSegments(totalSegments)
              //
              // .limit(256)
              //
              .build();

          if (limit > 0)
            scanRequest = scanRequest.toBuilder().limit(limit).build();

          debug("doSegment", segment, scanRequest);

          return lf(client.scan(scanRequest));
        }, scanResponse -> {

          debug("doSegment", segment, scanResponse.items().size());

          exclusiveStartKeys.set(segment, scanResponse.lastEvaluatedKey());

          // STEP 2 Process the scan
          List<ListenableFuture<?>> futures = new ArrayList<>();

          List<JsonElement> jsonElements = new ArrayList<>();
          for (Map<String, AttributeValue> item : scanResponse.items())
            jsonElements.add(parse(item));
          if (jsonElements.size()>0) {
            for (List<JsonElement> partition : Lists.partition(jsonElements, mtu>0?mtu:jsonElements.size())) {
              run(()->{
                ListenableFuture<?> lf = listener.apply(partition);
                futures.add(lf);
                return lf;
              });
            }  
          }

          run(()->{
            return Futures.successfulAsList(futures);
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
    Map<String, AttributeValue> sortedItem = new LinkedHashMap<String, AttributeValue>();
    for (String key : keySchema)
      sortedItem.put(key, item.get(key));
    // if (!options.keys)
      sortedItem.putAll(ImmutableSortedMap.copyOf(item));
    
    JsonElement jsonElement = new Gson().toJsonTree(Maps.transformValues(sortedItem, value -> {
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

  class Options {
    public int rcu;
    public int c; // concurrency, aka totalSegments
    public int limit; // scan request limit

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    public void infer(int provisionedRcu, int availableProcessors) {
      rcu = rcu > 0 ? rcu : provisionedRcu;
      c = c > 0 ? c : rcu > 0 ? (rcu + 127) / 128 : availableProcessors;
    }

    public String toString() {
      return new Gson().toJson(this);
    }
  }

  private final DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().build();

  @Override
  public boolean canActivate(String arg) {
    return ImmutableSet.of("dynamo", "dynamodb").contains(Args.base(arg).split(":")[0]);
  }

  @Override
  public InputPlugin activate(String arg) throws Exception {
    String tableName = Args.base(arg).split(":")[1];  
    Options options = Args.options(arg, Options.class);  
    debug("desired", options);

    DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
    DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();

    Iterable<String> keySchema = Lists.transform(describeTableResponse.table().keySchema(), e->e.attributeName());      

    int provisionedRcu = describeTableResponse.table().provisionedThroughput().readCapacityUnits().intValue();

    options.infer(provisionedRcu, Runtime.getRuntime().availableProcessors());
    debug("reported", options);

    RateLimiter readLimiter = RateLimiter.create(options.rcu>0?options.rcu:Integer.MAX_VALUE);

    return new DynamoInputPlugin(client, tableName, keySchema, readLimiter, options.c, options.limit);
  }
  
  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
