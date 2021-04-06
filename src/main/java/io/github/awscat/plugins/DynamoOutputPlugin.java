package io.github.awscat.plugins;

import java.net.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import org.springframework.stereotype.Service;
import org.springframework.util.*;

import helpers.*;
import io.github.awscat.*;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

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
    public String endpoint;
    public String toString() {
      return new Gson().toJson(this);
    }
  }

  private String tableName;
  private Options options;

  @Override
  public String help() {
      return "dynamo:<tableName>[,c,wcu,endpoint,delete]";
  }

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

    DynamoDbClientBuilder builder = DynamoDbClient.builder();    
    if (StringUtils.hasText(options.endpoint))
      builder.endpointOverride(URI.create(options.endpoint)).build();
    DynamoDbClient client = builder.build();

    DynamoDbAsyncClientBuilder asyncBuilder = DynamoDbAsyncClient.builder();    
    if (StringUtils.hasText(options.endpoint))
      asyncBuilder.endpointOverride(URI.create(options.endpoint)).build();
    DynamoDbAsyncClient asyncClient = asyncBuilder.build();

    Supplier<DescribeTableResponse> describeTable = Suppliers.memoizeWithExpiration(()->{
      return client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
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
      return new DynamoOutputPlugin(asyncClient, tableName, keySchema, sem, writeLimiter, options.delete);
    };
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}