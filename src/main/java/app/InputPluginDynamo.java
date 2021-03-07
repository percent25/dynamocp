package app;

import java.util.function.Function;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.io.BaseEncoding;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.utils.ImmutableMap;

public class InputPluginDynamo implements InputPlugin {

  private final DynamoDbAsyncClient client;
  private final String tableName;
  private final int totalSegments;
  private final RateLimiter readLimiter;

  public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList();

  private Function<JsonElement, ListenableFuture<?>> listener;

  // public static InputPlugin create(ApplicationArguments args, List<InputPlugin> plugins) throws Exception {
  //   String tableName = args.getNonOptionArgs().get(0);
  //   DynamoDbAsyncClient client = DynamoDbAsyncClient.create();
  //   DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
  //   DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();
  //   // describeTableResponse.table().
  //   return new InputPluginDynamo(client, tableName, 1, 128);
  // }

  /**
   * ctor
   * 
   * @param client
   * @param tableName
   * @param totalSegments
   * @param rcuLimit
   */
  public InputPluginDynamo(DynamoDbAsyncClient client, String tableName, int totalSegments, int rcuLimit) {
    this.client = client;
    this.tableName = tableName;
    this.totalSegments = totalSegments;
    this.readLimiter = RateLimiter.create(rcuLimit);

        // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
        exclusiveStartKeys.addAll(Collections.nCopies(Math.max(rcuLimit/128, 1), null));

  }

  @Override
  public void setListener(Function<JsonElement, ListenableFuture<?>> listener) {
    this.listener =listener;
  }

  @Override
  public ListenableFuture<?> read() throws Exception {
    return new FutureRunner() {
      {
        for (int segment = 0; segment < totalSegments; ++segment)
          doSegment(segment, 128);
      }
      void doSegment(int segment, int permits) {
        run(() -> {
          if (permits > 0)
            readLimiter.acquire(permits);

          // STEP 1 Do the scan
          ScanRequest scanRequest = ScanRequest.builder()
              //
              .tableName(tableName)
              //
              .exclusiveStartKey(exclusiveStartKeys.get(segment))
              //
              .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
              //
              .segment(segment)
              //
              .totalSegments(totalSegments)
              // //
              // .limit(options.scanLimit > 0 ? options.scanLimit : null)
              //
              .build();
          return lf(client.scan(scanRequest));
        }, scanResponse -> {
          try {

            exclusiveStartKeys.set(segment, scanResponse.lastEvaluatedKey());

            // STEP 2 Process results here

            // readCount.addAndGet(scanResponse.items().size());

            if (true)
            {


              for (Map<String, AttributeValue> item : scanResponse.items()) {
                // System.out.println(render(item));

                listener.apply(render(item)).get();
              }

                    // log(readCount.get(),
                    //     //
                    //     String.format("%s/%s", Double.valueOf(rcuMeter().getMeanRate()).intValue(),
                    //         Double.valueOf(wcuMeter().getMeanRate()).intValue()),
                    //     //
                    //     renderState(appState));

                  // System.err.println(renderState(appState));


              // for (Map<String, AttributeValue> item : scanResponse.items())
              //   out(item);

            } else {

                    // List<Map<String, AttributeValue>> items = new ArrayList<>();
                    // for (Map<String, AttributeValue> item : scanResponse.items()) {
                    //   item = new HashMap<>(item); // deep copy
                    //   items.add(item);
                    //   System.out.println(item);
                    // }
                    // doWrite(items);
            }

            if (!exclusiveStartKeys.get(segment).isEmpty())
              doSegment(segment, scanResponse.consumedCapacity().capacityUnits().intValue());

          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
    }.get();
  }

  private Map<String, AttributeValue> parse() {
    return null;
  }

  private JsonElement render(Map<String, AttributeValue> item) throws Exception {
    return new Gson().toJsonTree(Maps.transformValues(item, value -> {
      try {
        return new Gson().fromJson(new ObjectMapper().writeValueAsString(value.toBuilder()), JsonElement.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }));
  }

}


@Service
class InputPluginProviderDynamo implements InputPluginProvider {

  @Override
  public InputPlugin get(ApplicationArguments args) throws Exception {
    String tableName = args.getNonOptionArgs().get(0);
    DynamoDbAsyncClient client = DynamoDbAsyncClient.create();
    DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
    DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();
    // describeTableResponse.table().
    return new InputPluginDynamo(client, tableName, 1, 128);
  }
  
}
