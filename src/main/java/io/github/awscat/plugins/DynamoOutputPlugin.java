package io.github.awscat.plugins;

import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonElement;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import helpers.DynamoWriter;
import helpers.LogHelper;
import io.github.awscat.Args;
import io.github.awscat.Options;
import io.github.awscat.OutputPlugin;
import io.github.awscat.OutputPluginProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;

public class DynamoOutputPlugin implements OutputPlugin {

  private final DynamoWriter writer;

  public DynamoOutputPlugin(DynamoDbAsyncClient client, String tableName, Iterable<String> keySchema, boolean delete, RateLimiter writeLimiter) {
    debug("ctor");
    this.writer = new DynamoWriter(client, tableName, keySchema, delete, writeLimiter);
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

@Service
class DynamoOutputPluginProvider implements OutputPluginProvider {

  class DynamoOutputPluginProviderWork {

  }

  @Override
  public Supplier<OutputPlugin> get(String arg, ApplicationArguments args) throws Exception {
    //arn:aws:dynamodb:us-east-1:102938475610:table/MyTable
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

      //###TODO CONCURRENCY HERE??
      //###TODO CONCURRENCY HERE??
      //###TODO CONCURRENCY HERE??
      options.infer(Runtime.getRuntime().availableProcessors(), provisionedRcu, provisionedWcu);
      //###TODO CONCURRENCY HERE??
      //###TODO CONCURRENCY HERE??
      //###TODO CONCURRENCY HERE??
      debug("reported", options);

      RateLimiter writeLimiter = RateLimiter.create(options.wcu == 0 ? Integer.MAX_VALUE : options.wcu);
      debug("writeLimiter", writeLimiter);
      
      return () -> new DynamoOutputPlugin(client, tableName, keySchema, options.delete, writeLimiter);
    }
    return null;
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}