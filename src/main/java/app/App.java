package app;

import java.util.Optional;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ApplicationContext;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.utils.CompletableFutureUtils;
import software.amazon.awssdk.utils.ImmutableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

// import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
// import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
// import com.amazonaws.services.dynamodbv2.model.AttributeValue;
// import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
// import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
// import com.amazonaws.services.dynamodbv2.model.PutRequest;
// import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
// import com.amazonaws.services.dynamodbv2.model.ScanRequest;
// import com.amazonaws.services.dynamodbv2.model.ScanResult;
// import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

class AppState {
  public final AtomicLong count = new AtomicLong();
  public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList();
  // public AppState(long countSoFar) {
  //   this.countSoFar = countSoFar;
  // }
  // public AppState(long countSoFar, List<Map<String, AttributeValue>> exclusiveStartKeys) {
  //   this(countSoFar);
  //   this.exclusiveStartKeys.addAll(exclusiveStartKeys);
  // }
  public String toString() {
    return new Gson().toJson(this);
  }
}

// https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle
@SpringBootApplication
public class App implements ApplicationRunner {

  public static void main(String[] args) throws Exception {
    SpringApplication.run(App.class, args);
  }

  private final ApplicationContext context;
  private final Optional<BuildProperties> buildProperties;

  // aws sdk
  // private final AmazonDynamoDB dynamo = AmazonDynamoDBClientBuilder.defaultClient();

  // aws sdk 2
  private final DynamoDbClient dynamo = DynamoDbClient.create();
  private final DynamoDbAsyncClient dynamo2 = DynamoDbAsyncClient.create();

  private final List<Thread> threads = Lists.newArrayList();

  private AppState appState = new AppState();

  //###TODO REPLACE THIS W/EXCLUSIVESTARTKEYS.SIZE()??
  //###TODO REPLACE THIS W/EXCLUSIVESTARTKEYS.SIZE()??
  //###TODO REPLACE THIS W/EXCLUSIVESTARTKEYS.SIZE()??
  private final int withTotalSegments = 4;
  //###TODO REPLACE THIS W/EXCLUSIVESTARTKEYS.SIZE()??
  //###TODO REPLACE THIS W/EXCLUSIVESTARTKEYS.SIZE()??
  //###TODO REPLACE THIS W/EXCLUSIVESTARTKEYS.SIZE()??

  // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
  private final RateLimiter rateLimiter = RateLimiter.create(128.0);
  private final RateLimiter writeLimiter = RateLimiter.create(128.0);

  /**
   * ctor
   */
  public App(ApplicationContext context, Optional<BuildProperties> buildProperties) {
    this.context = context;
    this.buildProperties = buildProperties;
  }

  /**
   * run
   */
  @Override
  public void run(ApplicationArguments args) throws Exception {

    // source dynamo table name
    final String tableName = args.getNonOptionArgs().get(0);

    if (args.getOptionValues("rcu-limit") != null) {
      double rcu_limit = Double.parseDouble(args.getOptionValues("rcu-limit").iterator().next());
      log("rcu-limit", rcu_limit);
      rateLimiter.setRate(rcu_limit);
    }

    if (args.getOptionValues("wcu-limit") != null) {
      double wcu_limit = Double.parseDouble(args.getOptionValues("wcu-limit").iterator().next());
      log("wcu-limit", wcu_limit);
      writeLimiter.setRate(wcu_limit);
    }

    // --total-segments

    // --state
    if (args.getOptionValues("state")==null) {

      //###TODO can't differentiate between starting and finished
      //###TODO can't differentiate between starting and finished
      //###TODO can't differentiate between starting and finished
      appState.exclusiveStartKeys.addAll(Collections.nCopies(withTotalSegments, null));
      //###TODO can't differentiate between starting and finished
      //###TODO can't differentiate between starting and finished
      //###TODO can't differentiate between starting and finished
    } else {
      // restore state
      appState = parseState(args.getOptionValues("state").iterator().next());
      log(appState);
    }

    for (int segment = 0; segment < withTotalSegments; ++segment) {
      final int withSegment = segment;
      threads.add(new Thread() {
        @Override
        public void run() {

          log("run", withSegment);

          try {

            // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
            int permits = 128; // worst-case unbounded scan read capacity units (initial estimate)

            //###TODO doesn't need to be AtomicInteger
            //###TODO doesn't need to be AtomicInteger
            //###TODO doesn't need to be AtomicInteger
            // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html

            //###TODO how about 10000 - wcu_limit?
            //###TODO how about 10000 - wcu_limit?
            //###TODO how about 10000 - wcu_limit?

            final AtomicInteger writePermits = new AtomicInteger(25); // worst-case batch write capacity units (initial estimate)
            //###TODO doesn't need to be AtomicInteger
            //###TODO doesn't need to be AtomicInteger
            //###TODO doesn't need to be AtomicInteger

            do {
              rateLimiter.acquire(permits);

              // Do the scan
              ScanRequest scan = ScanRequest.builder()
                  //
                  .tableName(tableName)
                  //
                  .exclusiveStartKey(appState.exclusiveStartKeys.get(withSegment))
                  //
                  .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                  //
                  .segment(withSegment)
                  //
                  .totalSegments(withTotalSegments)
                  //
                  .build();

              ScanResponse result = dynamo.scan(scan);

              appState.exclusiveStartKeys.set(withSegment, result.lastEvaluatedKey());

              permits = new Double(result.consumedCapacity().capacityUnits()).intValue();

              // Process results here

              List<WriteRequest> writeRequests = Lists.newArrayList();
              for (Map<String, AttributeValue> item : result.items())
                writeRequests.add(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build());

              final int batch = 25;
              List<CompletableFuture<BatchWriteItemResponse>> batchWriteItemResponses = Lists.newArrayList();
              for (int fromIndex = 0; fromIndex < writeRequests.size(); fromIndex += batch) {
                // log(i);
                int toIndex = fromIndex + batch;
                if (writeRequests.size() < toIndex)
                  toIndex = writeRequests.size();
          
                List<WriteRequest> subList = writeRequests.subList(fromIndex, toIndex);
          
                // log(fromIndex, toIndex, subList.size());

                // Map<String, Collection<WriteRequest>> requestItems = Maps.newHashMap();
                // requestItems.put();

                BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                    .requestItems(ImmutableMap.of(tableName, subList))
                    //
                    .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    //
                    .build();

                // batchWriteItemRequest.addRequestItemsEntry(tableName, subList);

                // rate limit
                writeLimiter.acquire(writePermits.get());
                batchWriteItemResponses.add(dynamo2.batchWriteItem(batchWriteItemRequest));
              }

              CompletableFuture.allOf(batchWriteItemResponses.toArray(new CompletableFuture[0])).get();

              for (CompletableFuture<BatchWriteItemResponse> batchWriteItemResponse : batchWriteItemResponses) {
                writePermits.set(new Double(batchWriteItemResponse.get().consumedCapacity().iterator().next().capacityUnits()).intValue());
                // log("writePermits", writePermits.get());
              }
          
              appState.count.addAndGet(result.count());
              log(appState.count.get(), renderState(appState));

            } while (appState.exclusiveStartKeys.get(withSegment) != null);

          } catch (Exception e) {
            throw new RuntimeException(e);
          }

        }
      });

    }

    for (Thread thread : threads)
      thread.start();
    for (Thread thread : threads)
      thread.join();

  }

  private static AppState parseState(String base64) throws Exception {
    byte[] bytes = BaseEncoding.base64().decode(base64);
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    InputStream in = new GZIPInputStream(bais);
    String json = CharStreams.toString(new InputStreamReader(in));
    return new Gson().fromJson(json, AppState.class);
  }

  private static String renderState(AppState state) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStream out = new GZIPOutputStream(baos, true)) {
      out.write(new Gson().toJson(state).getBytes());
    }
    return BaseEncoding.base64().encode(baos.toByteArray());
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}
