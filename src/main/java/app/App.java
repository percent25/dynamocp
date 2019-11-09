package app;

import java.util.Optional;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ApplicationContext;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

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
  private final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newCopyOnWriteArrayList();
  private final AtomicLong totalCount = new AtomicLong();

  // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
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

    final int withTotalSegments = 5;
    for (int segment = 0; segment < withTotalSegments; ++segment) {
      exclusiveStartKeys.add(null);
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
                  .withExclusiveStartKey(exclusiveStartKeys.get(withSegment))
                  //
                  .withSegment(withSegment)
                  //
                  .withTotalSegments(withTotalSegments)
              //
              ;

              ScanResult result = dynamo.scan(scan);

              exclusiveStartKeys.set(withSegment, result.getLastEvaluatedKey());

              // double consumedCapacity = result.getConsumedCapacity().getCapacityUnits();
              // permitsToConsume = (int) (consumedCapacity - 1.0);
              // if (permitsToConsume <= 0) {
              //   permitsToConsume = 1;
              // }

              log(totalCount.addAndGet(result.getCount()), renderExclusiveStartKeys(exclusiveStartKeys));

            } while (exclusiveStartKeys.get(withSegment) != null);

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

  private String renderExclusiveStartKeys(List<Map<String, AttributeValue>> exclusiveStartKeys) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream out = new GZIPOutputStream(baos, true);
    out.write(new Gson().toJson(exclusiveStartKeys).getBytes());
    out.flush();
    return BaseEncoding.base64().encode(baos.toByteArray());
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}
