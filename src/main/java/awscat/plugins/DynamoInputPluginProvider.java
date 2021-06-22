package awscat.plugins;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.*;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import awscat.*;

import org.springframework.stereotype.Service;

import helpers.*;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html
class DynamoInputPlugin implements InputPlugin {

  private final DynamoDbAsyncClient client;
  private final String tableName;
  private final Iterable<String> keySchema;
  private final int totalSegments;
  private final AbstractThrottle readLimiter;
  private final int limit;

  //### TODO THIS DOES NOT NEED TO BE A MEMBER VARIABLE
  //### TODO THIS DOES NOT NEED TO BE A MEMBER VARIABLE
  //### TODO THIS DOES NOT NEED TO BE A MEMBER VARIABLE
  private final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList();
  //### TODO THIS DOES NOT NEED TO BE A MEMBER VARIABLE
  //### TODO THIS DOES NOT NEED TO BE A MEMBER VARIABLE
  //### TODO THIS DOES NOT NEED TO BE A MEMBER VARIABLE

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  /**
   * ctor
   * 
   * @param client
   * @param tableName
   * @param keySchema
   * @param totalSegments
   * @param readLimiter
   * @param limit
   */
  public DynamoInputPlugin(DynamoDbAsyncClient client, String tableName, Iterable<String> keySchema, int totalSegments, AbstractThrottle readLimiter, int limit) {
    debug("ctor");

    this.client = client;
    this.tableName = tableName;
    this.keySchema = keySchema;
    this.totalSegments = totalSegments;
    this.readLimiter = readLimiter;
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
        .add("totalSegments", totalSegments)
        //
        .add("readLimiter", readLimiter)
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
  public ListenableFuture<?> run(int mtu) throws Exception {
    debug("read", "mtu", mtu);

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
          //###TODO make async
          //###TODO make async
          //###TODO make async
          readLimiter.asyncAcquire(128).get(); //###TODO
          //###TODO make async
          //###TODO make async
          //###TODO make async

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

          debug("doSegment", segment, scanResponse.count());

          exclusiveStartKeys.set(segment, scanResponse.lastEvaluatedKey());

          // STEP 2 Process the scan
          List<ListenableFuture<?>> futures = new ArrayList<>();

          List<JsonElement> jsonElements = new ArrayList<>();
          for (Map<String, AttributeValue> item : scanResponse.items())
            jsonElements.add(MoreDynamo.parse(item));
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
            return Futures.allAsList(futures);
          }, result->{ // if success then continue
            if (!exclusiveStartKeys.get(segment).isEmpty()) //###TODO check for null here?
              doSegment(segment);
          });

        }, e->{
          debug("doSegment", segment, e);
          e.printStackTrace();
          throw new RuntimeException(e);
        });

      }
    };
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}

@Service
public class DynamoInputPluginProvider extends AbstractInputPluginProvider {

  class Options extends AwsOptions{
    public int c; // concurrency, aka totalSegments
    public int rcu;
    public int limit; // scan request limit
    public String toString() {
      return new Gson().toJson(this);
    }
  }

  private String tableName;
  private Options options;

  public DynamoInputPluginProvider() {
    super("dynamodb:<tableName>", Options.class);
  }

  public String toString() {
    // return new Gson().toJson(this);
    return MoreObjects.toStringHelper(this).add("tableName", tableName).add("options", options).toString();
  }

  @Override
  public boolean canActivate(String arg) {
    return ImmutableSet.of("dynamo", "dynamodb").contains(Addresses.base(arg).split(":")[0]);
  }

  @Override
  public InputPlugin activate(String arg) throws Exception {
    tableName = Addresses.base(arg).split(":")[1];  
    options = Addresses.options(arg, Options.class);  

    DynamoDbAsyncClient client = AwsHelper.create(DynamoDbAsyncClient.builder(), options);
    DynamoDbAsyncClient asyncClient = AwsHelper.create(DynamoDbAsyncClient.builder(), options);

    Supplier<DescribeTableResponse> describeTable = Suppliers.memoizeWithExpiration(()->{
      try {
        return client.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 25, TimeUnit.SECONDS);
    
    Iterable<String> keySchema = Lists.transform(describeTable.get().table().keySchema(), e->e.attributeName());      

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    int c = Runtime.getRuntime().availableProcessors();
    if (options.c > 0)
      c = options.c;
    else {
      if (options.rcu > 0)
        c = (options.rcu + 127) / 128;
    }

    AbstractThrottle readLimiter = new AbstractThrottleGuava(() -> {
      if (options.rcu > 0)
        return options.rcu;
      Number provisionedRcu = describeTable.get().table().provisionedThroughput().readCapacityUnits();
      if (provisionedRcu.longValue() > 0)
        return provisionedRcu;
      return Double.MAX_VALUE; // on-demand/pay-per-request
    });

    return new DynamoInputPlugin(asyncClient, tableName, keySchema, c, readLimiter, options.limit);
  }
  
  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
