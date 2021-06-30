package awscat.plugins;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import awscat.*;

import org.springframework.stereotype.Service;

import helpers.*;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

class DynamoInputPlugin implements InputPlugin {

  private final int limit;
  private final DynamoReader dynamoReader;

  /**
   * ctor
   * 
   * @param client
   * @param tableName
   * @param totalSegments
   * @param readLimiter
   * @param limit
   */
  public DynamoInputPlugin(DynamoDbAsyncClient client, String tableName, int totalSegments, AbstractThrottle readLimiter, int limit) {
    debug("ctor");
    this.limit = limit;
    dynamoReader = new DynamoReader(client, tableName, totalSegments, readLimiter);
  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    dynamoReader.setListener(listener);
  }

  @Override
  public ListenableFuture<?> run(int mtu) throws Exception {
    debug("run", "mtu", mtu);
    return dynamoReader.scan(mtu);
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
  public boolean canActivate(String address) {
    return ImmutableSet.of("dynamo", "dynamodb").contains(Addresses.base(address).split(":")[0]);
  }

  @Override
  public InputPlugin activate(String address) throws Exception {
    tableName = Addresses.base(address).split(":")[1];  
    options = Addresses.options(address, Options.class);  

    DynamoDbAsyncClient client = AwsHelper.create(DynamoDbAsyncClient.builder(), options);
    DynamoDbAsyncClient asyncClient = AwsHelper.create(DynamoDbAsyncClient.builder(), options);

    Supplier<DescribeTableResponse> describeTable = Suppliers.memoizeWithExpiration(()->{
      try {
        return client.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 25, TimeUnit.SECONDS);
    
    // Iterable<String> keySchema = Lists.transform(describeTable.get().table().keySchema(), e->e.attributeName());      

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

    return new DynamoInputPlugin(asyncClient, tableName, c, readLimiter, options.limit);
  }
  
  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
