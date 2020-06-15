package app;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
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

///###TODO --hash=Metric/
///###TODO --hash=Metric/
///###TODO --hash=Metric/

///###TODO --warmup-period=300
///###TODO --warmup-period=300
///###TODO --warmup-period=300

//###TODO --filter{javascript snippet??} (similar to --hash)
//###TODO --transform={javascript snippet??} (transform is a generalization of --filter)

class AppState {
  public final AtomicLong count = new AtomicLong();
  public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList(); // exclusiveStartKeys len *is* totalSegments
  public int totalSegments() {
    return exclusiveStartKeys.size();
  }
  public String toString() {
    return new Gson().toJson(this);
  }
}

class AppOptions {
  //
  public boolean debug;
  // reading
  public int rcuLimit = -1;
  // writing
  public int wcuLimit = -1;
  //
  public String resume; // base64 encoded gzipped app state
  //
  public String transform_expression;
  //
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

  private String sourceTable;
  private String targetTable;

  private AppState appState = new AppState();

  // aws sdk 2
  private final DynamoDbClient dynamo = DynamoDbClient.create();

  // @see totalSegments
  private final List<Thread> threads = Lists.newArrayList();

  /**
   * ctor
   */
  public App(ApplicationContext context, Optional<BuildProperties> buildProperties) {
    this.context = context;
    this.buildProperties = buildProperties;
  }

  private AppOptions parseOptions(ApplicationArguments args) {
    JsonObject options = new JsonObject();
    for (String name : args.getOptionNames()) {
      String lowerCamel = CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, name);
      options.addProperty(lowerCamel, true);
      for (String value : args.getOptionValues(name))
        options.addProperty(lowerCamel, value);
    }
    return new Gson().fromJson(options, AppOptions.class);
  }

  private RateLimiter rateLimiter;
  private RateLimiter writeLimiter;

  private final MetricRegistry metrics = new MetricRegistry();
  
  private Meter rcuMeter() { return metrics.meter("rcu"); }
  private Meter wcuMeter() { return metrics.meter("wcu"); }
  
  /**
   * run
   */
  @Override
  public void run(ApplicationArguments args) throws Exception {

    AppOptions options = parseOptions(args);

    log("desired", options);

    // source dynamo table name
    sourceTable = args.getNonOptionArgs().get(0);
    // targate dynamo table name
    if (args.getNonOptionArgs().size()>1)
      targetTable = args.getNonOptionArgs().get(1);

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    if (options.rcuLimit == -1)
      options.rcuLimit = options.wcuLimit == -1 ? 128 : options.wcuLimit / 8;
    if (options.wcuLimit == -1)
      options.wcuLimit = options.rcuLimit * 8;

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    appState.exclusiveStartKeys.addAll(Collections.nCopies(Math.max(options.rcuLimit/128, 1), null));

    if (options.resume!=null)
      appState = parseState(options.resume);

    log("effective", options);

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    rateLimiter = RateLimiter.create(options.rcuLimit);
    writeLimiter = RateLimiter.create(options.wcuLimit);

    log(Range.closedOpen(0, appState.totalSegments()));
    
    for (int segment = 0; segment < appState.exclusiveStartKeys.size(); ++segment) {
      final int finalSegment = segment;
      threads.add(new Thread() {

        // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
        int permits = 128; // worst-case unbounded scan read capacity units consumed (initial estimate)

        @Override
        public void run() {
          try {
            // log("run", finalSegment);
            do {
  
              if (permits > 0)
                rateLimiter.acquire(permits);

              // STEP 1 Do the scan
              ScanRequest scan = ScanRequest.builder()
                  //
                  .tableName(sourceTable)
                  //
                  .exclusiveStartKey(appState.exclusiveStartKeys.get(finalSegment))
                  //
                  .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                  //
                  .segment(finalSegment)
                  //
                  .totalSegments(appState.exclusiveStartKeys.size())
                  // //
                  // .limit(options.scanLimit > 0 ? options.scanLimit : null)
                  //
                  .build();

              ScanResponse result = dynamo.scan(scan);

              appState.exclusiveStartKeys.set(finalSegment, result.lastEvaluatedKey());

              Double consumedCapacityUnits = result.consumedCapacity().capacityUnits();

              rcuMeter().mark(consumedCapacityUnits.longValue());

              permits = consumedCapacityUnits.intValue();

              // STEP 2 Process results here

              if (targetTable == null) {
                
                for (Map<String, AttributeValue> item : result.items())
                  out(item);

              } else {

                List<Map<String, AttributeValue>> items = new ArrayList<>();
                for (Map<String, AttributeValue> item : result.items()) {
                  item = new HashMap<>(item); // deep copy
                  items.add(item);
                  //###TODO
                  //###TODO
                  //###TODO
                    // transform
                    // item.put("key", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
                  //###TODO
                  //###TODO
                  //###TODO
              }

                doWrite(items);

              }

              // log(appState.count.get(),
              // //
              // new Double(rcuMeter().getMeanRate()).intValue(),
              // //
              // new Double(wcuMeter().getMeanRate()).intValue(),
              // //
              // renderState(appState));

            } while (!appState.exclusiveStartKeys.get(finalSegment).isEmpty());

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

  private void doWrite(List<Map<String, AttributeValue>> items) throws Exception {
    // log("doWrite", items.size());
    List<WriteRequest> writeRequests = Lists.newArrayList();
    for (Map<String, AttributeValue> item : items)
      writeRequests.add(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build());

    // A single BatchWriteItem operation can contain up to 25 PutItem or DeleteItem requests.
    // The total size of all the items written cannot exceed 16 MB.
    final int batch = 25;
    for (int fromIndex = 0; fromIndex < writeRequests.size(); fromIndex += batch) {
      // log(i);
      int toIndex = fromIndex + batch;
      if (writeRequests.size() < toIndex)
        toIndex = writeRequests.size();

      List<WriteRequest> subList = writeRequests.subList(fromIndex, toIndex);

      BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
          //
          .requestItems(ImmutableMap.of(targetTable, subList))
          //
          .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
          //
          .build();

      // log("batchWriteItem", fromIndex, toIndex);

      BatchWriteItemResponse batchWriteItemResponse = dynamo.batchWriteItem(batchWriteItemRequest);
      Double consumedCapacityUnits = batchWriteItemResponse.consumedCapacity().iterator().next().capacityUnits();
      wcuMeter().mark(consumedCapacityUnits.longValue());

      int writePermits = consumedCapacityUnits.intValue();
      if (writePermits > 0)
        writeLimiter.acquire(writePermits);
    }

    appState.count.addAndGet(items.size());

    log(appState.count.get(),
        //
        String.format("%s/%s", Double.valueOf(rcuMeter().getMeanRate()).intValue(), Double.valueOf(wcuMeter().getMeanRate()).intValue()),
        //
        renderState(appState));
  }

  private static AppState parseState(String base64) throws Exception {
    byte[] bytes = BaseEncoding.base64().decode(base64);
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    InputStream in = new GZIPInputStream(bais);
    String json = CharStreams.toString(new InputStreamReader(in));

    AppState state = new Gson().fromJson(json, AppState.class);

    // sigh.
    for (Map<String, AttributeValue> exclusiveStartKey : state.exclusiveStartKeys) {
      exclusiveStartKey.putAll(Maps.transformValues(exclusiveStartKey, (value) -> {
        //###TODO handle all types
        //###TODO handle all types
        //###TODO handle all types
        return AttributeValue.builder().s(value.s()).build();
      }));
    }

    return state;
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

  private void out(Object... args) {
    List<String> parts = new ArrayList<>();
    for (Object arg : args)
      parts.add("" + arg);
    System.out.println(String.join(" ", parts));
  }

}
