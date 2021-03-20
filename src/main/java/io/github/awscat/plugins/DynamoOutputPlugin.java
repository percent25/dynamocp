package io.github.awscat.plugins;

import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
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

  public DynamoOutputPlugin(DynamoDbAsyncClient client, String tableName, boolean delete, Iterable<String> keySchema, RateLimiter writeLimiter) {
    debug("ctor");
    this.writer = new DynamoWriter(client, tableName, delete, keySchema, writeLimiter);
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

  private final ApplicationArguments args;
  private String tableName;
  private DynamoOptions desiredOptions;
  private DynamoOptions reportedOptions;
  
  private final DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().build();
  
  private Iterable<String> keySchema;
  private int concurrency;
  private int provisionedRcu;
  private int provisionedWcu;
  private RateLimiter writeLimiter;

  private Exception e;

  /**
   * ctor
   * 
   * @param args
   */
  public DynamoOutputPluginProvider(ApplicationArguments args) {
    this.args = args;
  }

  public String toString() {
    return MoreObjects.toStringHelper(this).add("tableName", tableName).add("desiredOptions", desiredOptions).add("reportedOptions", reportedOptions).toString();
  }

  @Override
  public int mtu() {
    return 25;
  }

  @Override
  public boolean canActivate() {
    String arg = args.getNonOptionArgs().get(1);
    tableName = match("dynamo:(.+)", Args.base(arg)).group(1);
    desiredOptions = Args.options(arg, DynamoOptions.class);
    reportedOptions = Args.options(arg, DynamoOptions.class);
    try {
      DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
      DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();
  
      keySchema = Lists.transform(describeTableResponse.table().keySchema(), e->e.attributeName());      
  
      concurrency = Runtime.getRuntime().availableProcessors();
      provisionedRcu = describeTableResponse.table().provisionedThroughput().readCapacityUnits().intValue();
      provisionedWcu = describeTableResponse.table().provisionedThroughput().writeCapacityUnits().intValue();
  
      //###TODO CONCURRENCY HERE??
      //###TODO CONCURRENCY HERE??
      //###TODO CONCURRENCY HERE??
      reportedOptions.infer(concurrency, provisionedRcu, provisionedWcu);
      //###TODO CONCURRENCY HERE??
      //###TODO CONCURRENCY HERE??
      //###TODO CONCURRENCY HERE??
  
      writeLimiter = RateLimiter.create(reportedOptions.wcu > 0 ? reportedOptions.wcu : Integer.MAX_VALUE);
  
    } catch (Exception e) {
      this.e = e;
    }
    return true; // we got this far
  }

  @Override
  public Supplier<OutputPlugin> get() throws Exception {
    if (e != null)
      throw e;
    return () -> new DynamoOutputPlugin(client, tableName, reportedOptions.delete, keySchema, writeLimiter);
  }

  private Matcher match(String regex, CharSequence input) {
    var m = Pattern.compile(regex).matcher(input);
    m.matches();
    return m;
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}