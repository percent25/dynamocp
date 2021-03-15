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

  public DynamoOutputPlugin(DynamoDbAsyncClient client, String tableName, RateLimiter writeLimiter, boolean delete, Iterable<String> keySchema) {
    this.writer = new DynamoWriter(client, tableName, writeLimiter, delete, keySchema);
  }

  @Override
  public ListenableFuture<?> write(JsonElement jsonElement) {
    return writer.write(jsonElement);
  }

  @Override
  public ListenableFuture<?> flush() {
    return writer.flush();
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }
}

@Service
class DynamoOutputPluginProvider implements OutputPluginProvider {

  class DynamoOutputPluginProviderWork {

  }

  @Override
  public Supplier<OutputPlugin> get(String arg, ApplicationArguments args) throws Exception {
    if (arg.startsWith("dynamo:")) {
      String tableName = Args.parseArg(arg).split(":")[1];

      DynamoOptions options = Options.parse(arg, DynamoOptions.class);
      log("desired", options);
    
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
      log("reported", options);

      RateLimiter writeLimiter = RateLimiter.create(options.wcu == 0 ? Integer.MAX_VALUE : options.wcu);
      log("writeLimiter", writeLimiter);
      
      return () -> new DynamoOutputPlugin(client, tableName, writeLimiter, options.delete, keySchema);
    }
    return null;
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}