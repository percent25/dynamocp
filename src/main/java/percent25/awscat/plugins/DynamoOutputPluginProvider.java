package percent25.awscat.plugins;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import org.springframework.stereotype.Service;
import org.springframework.util.*;

import helpers.*;
import percent25.awscat.*;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * DynamoOutputPlugin
 */
class DynamoOutputPlugin implements OutputPlugin {

  private final DynamoWriter writer;

  public DynamoOutputPlugin(DynamoDbAsyncClient client, String tableName, List<KeySchemaElement> keySchema, Semaphore c, AbstractThrottle writeLimiter, boolean delete) {
    debug("ctor");
    this.writer = new DynamoWriter(client, tableName, keySchema, c, writeLimiter, delete);
  }

  @Override
  public ListenableFuture<?> write(JsonElement jsonElement) {
    return writer.write(jsonElement);
  }

  @Override
  public ListenableFuture<?> flush() {
    return writer.flush();
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }
}

/**
 * DynamoOutputPluginProvider
 */
@Service
public class DynamoOutputPluginProvider implements OutputPluginProvider {

  class Options extends AwsOptions {
    // concurrency
    public int c;
    // write capacity units
    public int wcu;
    // putItem vs deleteItem
    public boolean delete;
    public String toString() {
      return new Gson().toJson(this);
    }
  }

  private String tableName;
  private Options options;

  @Override
  public String help() {
      return "dynamo:<tableName>[,c,wcu,endpoint,profile,delete]";
  }

  @Override
  public int mtuHint() {
    return 25;
  }

  public String toString() {
    return new Gson().toJson(this);
  }

  @Override
  public boolean canActivate(String address) {
    return ImmutableSet.of("dynamo", "dynamodb").contains(address.split(":")[0]);
  }

  @Override
  public Supplier<OutputPlugin> activate(String address) throws Exception {
    tableName = Addresses.base(address).split(":")[1];
    options = Addresses.options(address, Options.class);

    DynamoDbAsyncClient client = AwsHelper.buildAsync(DynamoDbAsyncClient.builder(), options);
    Supplier<DescribeTableResponse> describeTableResponseSupplier = Suppliers.memoizeWithExpiration(()->{
      try {
        return client.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 25, TimeUnit.SECONDS);

    List<KeySchemaElement> keySchema = describeTableResponseSupplier.get().table().keySchema();

    options.c = options.c > 0 ? options.c : Runtime.getRuntime().availableProcessors();

    Semaphore sem = new Semaphore(options.c);

    AbstractThrottle writeLimiter = new AbstractThrottleGuava(() -> {
      if (options.wcu > 0)
        return options.wcu;
      Number provisionedWcu = describeTableResponseSupplier.get().table().provisionedThroughput().writeCapacityUnits();
      if (provisionedWcu.longValue() > 0)
        return provisionedWcu;
      return Double.MAX_VALUE; // on-demand/pay-per-request
    });

    DynamoDbAsyncClient asyncClient = AwsHelper.buildAsync(DynamoDbAsyncClient.builder(), options);
    return ()->{
      return new DynamoOutputPlugin(asyncClient, tableName, keySchema, sem, writeLimiter, options.delete);
    };
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}