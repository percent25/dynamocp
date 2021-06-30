package awscat.plugins;

import java.net.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import awscat.*;

import org.springframework.stereotype.Service;
import org.springframework.util.*;

import helpers.*;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * DynamoOutputPlugin
 */
class DynamoOutputPlugin implements OutputPlugin {

  private final DynamoWriter writer;

  public DynamoOutputPlugin(DynamoDbAsyncClient client, String tableName, Iterable<String> keySchema, Semaphore c, AbstractThrottle writeLimiter, boolean delete) {
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
    // issue deleteItem (vs putItem)
    public boolean delete;
    public String toString() {
      return new Gson().toJson(this);
    }
  }

  private String tableName;
  private Options options;

  @Override
  public String help() {
      return "dynamodb:<tableName>[,c,wcu,endpoint,profile,delete]";
  }

  @Override
  public int mtu() {
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

    options.c = options.c > 0 ? options.c : Runtime.getRuntime().availableProcessors();

    Semaphore sem = new Semaphore(options.c);

    AbstractThrottle writeLimiter = new AbstractThrottleGuava(() -> {
      if (options.wcu > 0)
        return options.wcu;
      Number provisionedWcu = describeTable.get().table().provisionedThroughput().writeCapacityUnits();
      if (provisionedWcu.longValue() > 0)
        return provisionedWcu;
      return Double.MAX_VALUE; // on-demand/pay-per-request
    });

    return ()->{
      return new DynamoOutputPlugin(asyncClient, tableName, keySchema, sem, writeLimiter, options.delete);
    };
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}