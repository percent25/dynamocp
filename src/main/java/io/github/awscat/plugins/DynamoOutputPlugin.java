package io.github.awscat.plugins;

import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

import com.google.common.base.Defaults;
import com.google.common.base.MoreObjects;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import helpers.DynamoWriter;
import helpers.LogHelper;
import io.github.awscat.Args;
import io.github.awscat.OutputPlugin;
import io.github.awscat.OutputPluginProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;

/**
 * DynamoOutputPlugin
 */
public class DynamoOutputPlugin implements OutputPlugin {

  private final DynamoWriter writer;

  public DynamoOutputPlugin(DynamoDbAsyncClient client, String tableName, Iterable<String> keySchema, Semaphore c, RateLimiter writeLimiter, boolean delete) {
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
    public int c;
    public int wcu;
    public boolean delete;
    public String toString() {
      return new Gson().toJson(this);
    }
  }

  @Override
  public int mtu() {
    return 25;
  }

  @Override
  public boolean canActivate(String arg) {
    return ImmutableSet.of("dynamo", "dynamodb").contains(arg.split(":")[0]);
  }

  @Override
  public Supplier<OutputPlugin> activate(String arg) throws Exception {
    String tableName = Args.base(arg).split(":")[1];
    Options options = Args.options(arg, Options.class);

    DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().build();
  
    DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
    DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();

    Iterable<String> keySchema = Lists.transform(describeTableResponse.table().keySchema(), e->e.attributeName());      

    int availableProcessors = Runtime.getRuntime().availableProcessors();
    int provisionedWcu = describeTableResponse.table().provisionedThroughput().writeCapacityUnits().intValue();

    options.c = options.c > 0 ? options.c : availableProcessors;
    options.wcu = options.wcu > 0 ? options.wcu : provisionedWcu;

    var c = new Semaphore(options.c);
    var writeLimiter = RateLimiter.create(options.wcu > 0 ? options.wcu : Integer.MAX_VALUE);

    Supplier<?> preWarm = Suppliers.memoize(() -> {
      if (options.wcu > 0)
        writeLimiter.acquire(options.wcu);
      return Defaults.defaultValue(Void.class);
    });

    return new Supplier<OutputPlugin>() {
      @Override
      public OutputPlugin get() {
        //###TODO REALLY NEEDED??
        //###TODO REALLY NEEDED??
        //###TODO REALLY NEEDED??
        preWarm.get();
        //###TODO REALLY NEEDED??
        //###TODO REALLY NEEDED??
        //###TODO REALLY NEEDED??
        return new DynamoOutputPlugin(client, tableName, keySchema, c, writeLimiter, options.delete);
      }
      public String toString() {
        return MoreObjects.toStringHelper("DynamoOutputPlugin")
            //
            .add("tableName", tableName)
            //
            .add("keySchema", keySchema)
            //
            .add("c", options.c)
            //
            .add("wcu", options.wcu)
            //
            .add("delete", options.delete)
            //
            .toString();
      }
    };
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}