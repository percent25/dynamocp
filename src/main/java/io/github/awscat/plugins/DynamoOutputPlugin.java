package io.github.awscat.plugins;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.springframework.stereotype.Service;

import helpers.AbstractThrottle;
import helpers.AbstractThrottleGuava;
import helpers.DynamoWriter;
import helpers.LogHelper;
import io.github.awscat.Args;
import io.github.awscat.OutputPlugin;
import io.github.awscat.OutputPluginProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;

/**
 * DynamoOutputPlugin
 */
public class DynamoOutputPlugin implements OutputPlugin {

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
class DynamoOutputPluginProvider implements OutputPluginProvider {

  class Options {
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
  public int mtu() {
    return 25;
  }

  @Override
  public boolean canActivate(String arg) {
    return ImmutableSet.of("dynamo", "dynamodb").contains(arg.split(":")[0]);
  }

  public String toString() {
    return new Gson().toJson(this);
  }

  @Override
  public Supplier<OutputPlugin> activate(String arg) throws Exception {
    tableName = Args.base(arg).split(":")[1];
    options = Args.options(arg, Options.class);
    DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().build();

    Supplier<DescribeTableResponse> describeTable = Suppliers.memoizeWithExpiration(()->{
      return DynamoDbClient.create().describeTable(DescribeTableRequest.builder().tableName(tableName).build());
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
      return Double.MAX_VALUE;
    });

    return ()->{
      return new DynamoOutputPlugin(client, tableName, keySchema, sem, writeLimiter, options.delete);
    };
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}