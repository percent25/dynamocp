package app;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

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
import software.amazon.awssdk.utils.ImmutableMap;

class AppState {
  public final AtomicLong count = new AtomicLong();
  //###TODO COPYONWRITEARRAYLIST??
  //###TODO COPYONWRITEARRAYLIST??
  //###TODO COPYONWRITEARRAYLIST??
  public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList(); // exclusiveStartKeys len *is* totalSegments
  //###TODO COPYONWRITEARRAYLIST??
  //###TODO COPYONWRITEARRAYLIST??
  //###TODO COPYONWRITEARRAYLIST??
  // public AppState(long countSoFar, List<Map<String, AttributeValue>> exclusiveStartKeys) {
  //   this(countSoFar);
  //   this.exclusiveStartKeys.addAll(exclusiveStartKeys);
  // }
  public String toString() {
    return new Gson().toJson(this);
  }
}

class AppOptions {
  public boolean debug;
  public double rcuLimit = 128.0;
  public double wcuLimit = 128.0;
  public int scanLimit = -1;
  public int totalSegments = 4;
  public String resume; // base64 encoded
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

  private AppState appState = new AppState();

  // aws sdk 2
  private final DynamoDbClient dynamo = DynamoDbClient.create();
  private final DynamoDbAsyncClient dynamo2 = DynamoDbAsyncClient.builder().build();

  // @see totalSegments
  private final List<Thread> threads = Lists.newArrayList();

  /**
   * ctor
   */
  public App(ApplicationContext context, Optional<BuildProperties> buildProperties) {
    this.context = context;
    this.buildProperties = buildProperties;
  }

  private AppOptions getOptions(ApplicationArguments args) {
    JsonObject options = new JsonObject();
    for (String name : args.getOptionNames()) {
      String lowerCamel = CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, name);
      options.addProperty(lowerCamel, true);
      for (String value : args.getOptionValues(name))
        options.addProperty(lowerCamel, value);
    }
    return new Gson().fromJson(options, AppOptions.class);
  }

  /**
   * run
   */
  @Override
  public void run(ApplicationArguments args) throws Exception {

    AppOptions options = getOptions(args);

    log(options);

    // source dynamo table name
    final String tableName = args.getNonOptionArgs().get(0);

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    final RateLimiter rateLimiter = RateLimiter.create(options.rcuLimit);
    final RateLimiter writeLimiter = RateLimiter.create(options.wcuLimit);

    // final AppState appState = options.resume!=null?parseState(options.resume):new AppState(options.totalSegments);

    if (options.resume!=null) {
      log("totalSegments ignored");
      appState = parseState(options.resume);
      log(appState);
    } else {
      appState.exclusiveStartKeys.addAll(Collections.nCopies(options.totalSegments, null));
    }
 
          // // --state
          // if (args.getOptionValues("state")==null) {

          //   //###TODO can't differentiate between starting and finished
          //   //###TODO can't differentiate between starting and finished
          //   //###TODO can't differentiate between starting and finished
          //   appState.exclusiveStartKeys.addAll(Collections.nCopies(options.totalSegments, null));
          //   //###TODO can't differentiate between starting and finished
          //   //###TODO can't differentiate between starting and finished
          //   //###TODO can't differentiate between starting and finished
          
          // } else {
          //   // restore state
          //   appState = parseState(args.getOptionValues("state").iterator().next());
          //   log(appState);
          // }

    for (int segment = 0; segment < appState.exclusiveStartKeys.size(); ++segment) {
      final int withSegment = segment;
      threads.add(new Thread() {

            // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
            int permits = 128; // worst-case unbounded scan read capacity units (initial estimate)

                        //###TODO doesn't need to be AtomicInteger
            //###TODO doesn't need to be AtomicInteger
            //###TODO doesn't need to be AtomicInteger
            // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html

            //###TODO how about 10000 - wcu_limit?
            //###TODO how about 10000 - wcu_limit?
            //###TODO how about 10000 - wcu_limit?

            int writePermits = 25; // worst-case batch write capacity units (initial estimate)
            //###TODO doesn't need to be AtomicInteger
            //###TODO doesn't need to be AtomicInteger
            //###TODO doesn't need to be AtomicInteger

        @Override
        public void run() {

          log("run", withSegment);

          try {


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
                  .totalSegments(appState.exclusiveStartKeys.size())
                  //
                  .limit(options.scanLimit > 0 ? options.scanLimit : null)
                  //
                  .build();

              ScanResponse result = dynamo.scan(scan);

              appState.exclusiveStartKeys.set(withSegment, result.lastEvaluatedKey());

              permits = Math.max(new Double(result.consumedCapacity().capacityUnits()).intValue(), 1);

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

                BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                    .requestItems(ImmutableMap.of(tableName, subList))
                    //
                    .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    //
                    .build();

                // rate limit
                writeLimiter.acquire(writePermits);

                batchWriteItemResponses.add(dynamo2.batchWriteItem(batchWriteItemRequest));
              }

              CompletableFuture.allOf(batchWriteItemResponses.toArray(new CompletableFuture[0])).get();

              for (CompletableFuture<BatchWriteItemResponse> batchWriteItemResponse : batchWriteItemResponses) {
                // log("writePermits", writePermits;
                writePermits = Math.max(new Double(batchWriteItemResponse.get().consumedCapacity().iterator().next().capacityUnits()).intValue(), 1);
              }
          
              appState.count.addAndGet(result.count());
              log(appState.count.get(), renderState(appState));

            } while (!appState.exclusiveStartKeys.get(withSegment).isEmpty());

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
