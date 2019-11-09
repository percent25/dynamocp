package app;

import java.util.Optional;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ApplicationContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

class AppState {
  public long countSoFar = 0;
  public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList();
  public AppState(long countSoFar) {
    this.countSoFar = countSoFar;
  }
  public AppState(long countSoFar, List<Map<String, AttributeValue>> exclusiveStartKeys) {
    this(countSoFar);
    this.exclusiveStartKeys.addAll(exclusiveStartKeys);
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

  private final AmazonDynamoDB dynamo = AmazonDynamoDBClientBuilder.defaultClient();
  private final List<Thread> threads = Lists.newArrayList();

  private AppState appState = new AppState(0);

  // private final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newCopyOnWriteArrayList();
  // private final AtomicLong totalCount = new AtomicLong();

  // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
  private final int withTotalSegments = 5;
  private final RateLimiter rateLimiter = RateLimiter.create(128.0);

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
    // private final String tableName = "dev-metrics-test";
    // private final String tableName = "dev-MetricsTable2972A477-X02V6T2YSF22";

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    if (args.getOptionValues("rcu-limit")!=null)
      rateLimiter.setRate(Double.parseDouble(args.getOptionValues("rcu-limit").iterator().next()));

    // --total-segments

    // --state
    if (args.getOptionValues("state")==null) {
      //###TODO can't differentiate between starting and finished
      //###TODO can't differentiate between starting and finished
      //###TODO can't differentiate between starting and finished
      for (int segment = 0; segment < withTotalSegments; ++segment)
        appState.exclusiveStartKeys.add(null);
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

          log("run");

          try {

            do {
              // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
              rateLimiter.acquire(128);

              // Do the scan
              ScanRequest scan = new ScanRequest()
                  //
                  .withTableName(tableName)
                  //
                  .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                  //
                  .withExclusiveStartKey(appState.exclusiveStartKeys.get(withSegment))
                  //
                  .withSegment(withSegment)
                  //
                  .withTotalSegments(withTotalSegments)
              //
              ;

              ScanResult result = dynamo.scan(scan);

              appState.exclusiveStartKeys.set(withSegment, result.getLastEvaluatedKey());

              // double consumedCapacity = result.getConsumedCapacity().getCapacityUnits();
              // permitsToConsume = (int) (consumedCapacity - 1.0);
              // if (permitsToConsume <= 0) {
              //   permitsToConsume = 1;
              // }

              appState.countSoFar += result.getCount();

              log(renderState(appState));

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
