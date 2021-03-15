package io.github.awscat.plugins;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
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

public class DynamoInputPlugin implements InputPlugin {

  private final DynamoDbAsyncClient client;
  private final String tableName;
  private final Iterable<String> keySchema;
  private final DynamoOptions options;
  private final RateLimiter readLimiter;

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList();

  // thundering herd
  private final BlockingQueue<Number> permitsQueue;

  /**
   * ctor
   * 
   * @param client
   * @param tableName
   * @param keySchema
   * @param options
   */
  public DynamoInputPlugin(DynamoDbAsyncClient client, String tableName, Iterable<String> keySchema, DynamoOptions options) {
    debug("ctor", tableName, keySchema, options);

    this.client = client;
    this.tableName = tableName;
    this.keySchema = keySchema;
    this.options = options;
  
    this.readLimiter = RateLimiter.create(options.rcu==0?Integer.MAX_VALUE:options.rcu);

    permitsQueue = Queues.newArrayBlockingQueue(options.totalSegments());

    for (int i = 0; i < options.totalSegments(); ++i)
      permitsQueue.add(128); // not rcuLimit

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    exclusiveStartKeys.addAll(Collections.nCopies(options.totalSegments(), null));

  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  @Override
  public ListenableFuture<?> read() throws Exception {
    return new FutureRunner() {
      {
        for (int segment = 0; segment < options.totalSegments(); ++segment)
          doSegment(segment);
      }
      void doSegment(int segment) {
        
        debug(segment, "doSegment");

        int[] permits = new int[1];
        List<JsonElement> jsonElements = new ArrayList<>();

        run(()->{
          return Futures.submit(()->{
            return permitsQueue.take();
          }, MoreExecutors.directExecutor());
        }, number->{

          permits[0]=number.intValue();

          run(() -> {
  
            if (permits[0] > 0)
              readLimiter.acquire(permits[0]);
  
            // STEP 1 Do the scan
            ScanRequest scanRequest = ScanRequest.builder()
                //
                .tableName(tableName)
                //
                .exclusiveStartKey(exclusiveStartKeys.get(segment))
                //
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
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
  
            debug(segment, "scanRequest", scanRequest);
  
            return lf(client.scan(scanRequest));
          }, scanResponse -> {
  
            debug(segment, "scanResponse", scanResponse.items().size());
  
            exclusiveStartKeys.set(segment, scanResponse.lastEvaluatedKey());
  
            permits[0] = scanResponse.consumedCapacity().capacityUnits().intValue();
  
            // STEP 2 Process results here
  
            // readCount.addAndGet(scanResponse.items().size());
  
            for (Map<String, AttributeValue> item : scanResponse.items()) {
              jsonElements.add(parse(item));
              // System.out.println(render(item));
            }
  
                  // log(readCount.get(),
                  //     //
                  //     String.format("%s/%s", Double.valueOf(rcuMeter().getMeanRate()).intValue(),
                  //         Double.valueOf(wcuMeter().getMeanRate()).intValue()),
                  //     //
                  //     renderState(appState));
  
                // System.err.println(renderState(appState));
  
          }, e->{
            debug(e);
            e.printStackTrace();
            throw new RuntimeException(e);
          }, ()->{
            try {
              permitsQueue.put(permits[0]); // produce permits
            } catch (Exception e) {
              debug(segment, e);
              e.printStackTrace();
              throw new RuntimeException(e);
            } finally {
              run(()->{
                return listener.apply(jsonElements);
              }, ()->{ // finally
                if (!exclusiveStartKeys.get(segment).isEmpty())
                  doSegment(segment); // consume permits
              });
            }
          });
  
        });

      }
    }.get();
  }

  private JsonElement parse(Map<String, AttributeValue> itemIn) {
    
    // aesthetics
    var itemOut = new LinkedHashMap<String, AttributeValue>();
    for (var key : keySchema)
      itemOut.put(key, itemIn.get(key));
    if (!options.keys)
      itemOut.putAll(itemIn);
    
    var jsonElement = new Gson().toJsonTree(Maps.transformValues(itemOut, value -> {
      try {
        return new Gson().fromJson(new ObjectMapper().writeValueAsString(value.toBuilder()), JsonElement.class);
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

  @Override
  public InputPlugin get(String arg, ApplicationArguments args) throws Exception {
    if (arg.startsWith("dynamo:")) {

      String tableName = Args.parseArg(arg).split(":")[1];

      DynamoOptions options = Options.parse(arg, DynamoOptions.class);  
      debug("desired", options);
  
      DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().build();
      DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
      DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();

      Iterable<String> keySchema = Lists.transform(describeTableResponse.table().keySchema(), e->e.attributeName());      

      int provisionedRcu = describeTableResponse.table().provisionedThroughput().readCapacityUnits().intValue();
      int provisionedWcu = describeTableResponse.table().provisionedThroughput().writeCapacityUnits().intValue();

      options.infer(Runtime.getRuntime().availableProcessors(), provisionedRcu, provisionedWcu);
      debug("reported", options);

      //###TODO PASS THIS TO DYNAMOINPUTPLUGIN
      //###TODO PASS THIS TO DYNAMOINPUTPLUGIN
      //###TODO PASS THIS TO DYNAMOINPUTPLUGIN
      RateLimiter readLimiter = RateLimiter.create(options.rcu==0?Integer.MAX_VALUE:options.rcu);
      debug("readLimiter", readLimiter);
      //###TODO PASS THIS TO DYNAMOINPUTPLUGIN
      //###TODO PASS THIS TO DYNAMOINPUTPLUGIN
      //###TODO PASS THIS TO DYNAMOINPUTPLUGIN

      return new DynamoInputPlugin(client, tableName, keySchema, options);
    }
    return null;
  }
  
  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
