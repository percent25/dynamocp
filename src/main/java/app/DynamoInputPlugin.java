package app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

public class DynamoInputPlugin implements InputPlugin {

  private final DynamoDbAsyncClient client;
  private final String tableName;
  private final int rcuLimit;
  private final int totalSegments;
  private final RateLimiter readLimiter;

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList();

  // thundering herd
  private final BlockingQueue<Number> permitsQueue;

  private static ExecutorService executor = Executors.newCachedThreadPool();

  /**
   * ctor
   * 
   * @param client
   * @param tableName
   * @param totalSegments
   * @param rcuLimit
   */
  public DynamoInputPlugin(DynamoDbAsyncClient client, String tableName, int rcuLimit, int totalSegments) {
    log("ctor", tableName, rcuLimit, totalSegments);

    this.client = client;
    this.tableName = tableName;
    this.rcuLimit = rcuLimit;
    this.totalSegments = totalSegments;
  
    this.readLimiter = RateLimiter.create(rcuLimit);

    permitsQueue = Queues.newArrayBlockingQueue(totalSegments);

    for (int i = 0; i < totalSegments; ++i)
      permitsQueue.add(128); // not rcuLimit

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    exclusiveStartKeys.addAll(Collections.nCopies(totalSegments, null));

  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  @Override
  public ListenableFuture<?> read() throws Exception {
    return new FutureRunner() {
      {
        for (int segment = 0; segment < totalSegments; ++segment)
          doSegment(segment);
      }
      void doSegment(int segment) {
        
        log(segment, "doSegment");

        int[] permits = new int[1];
        List<JsonElement> jsonElements = new ArrayList<>();

        run(()->{
          return Futures.submit(()->{
            return permitsQueue.take();
          }, executor);
        }, number->{

          permits[0]=number.intValue();

          run(() -> {
            // permits[0] = permitsQueue.take().intValue();
            log(segment, "permits:"+permits[0]);
  
            if (permits[0] > 0)
              readLimiter.acquire(permits[0]);
            log(segment, "acquired:"+permits[0]);
  
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
                //
                // .limit(256)
                //
                .build();
  
            log(segment, "scanRequest", scanRequest);
  
            return lf(client.scan(scanRequest));
          }, scanResponse -> {
  
            log(segment, "scanResponse", scanResponse.items().size());
  
            exclusiveStartKeys.set(segment, scanResponse.lastEvaluatedKey());
  
            permits[0] = scanResponse.consumedCapacity().capacityUnits().intValue();
  
            // STEP 2 Process results here
  
            // readCount.addAndGet(scanResponse.items().size());
  
            for (Map<String, AttributeValue> item : scanResponse.items()) {
              jsonElements.add(parse(item));
              // System.out.println(render(item));
            }
  
                  // log(readCount.get(),
                  //     //
                  //     String.format("%s/%s", Double.valueOf(rcuMeter().getMeanRate()).intValue(),
                  //         Double.valueOf(wcuMeter().getMeanRate()).intValue()),
                  //     //
                  //     renderState(appState));
  
                // System.err.println(renderState(appState));
  
          }, e->{
            log(e);
            e.printStackTrace();
            throw new RuntimeException(e);
          }, ()->{
            try {
              permitsQueue.put(permits[0]); // produce permits
            } catch (Exception e) {
              log(segment, e);
              e.printStackTrace();
              throw new RuntimeException(e);
            } finally {
              run(()->{
                return listener.apply(jsonElements);
              }, ()->{ // finally
                if (!exclusiveStartKeys.get(segment).isEmpty())
                  doSegment(segment); // consume permits
              });
            }
          });
  
        });

      }
    }.get();
  }

  private JsonElement parse(Map<String, AttributeValue> item) {
    return new Gson().toJsonTree(Maps.transformValues(item, value -> {
      try {
        return new Gson().fromJson(new ObjectMapper().writeValueAsString(value.toBuilder()), JsonElement.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }));
  }

  private void log(Object... args) {
    String threadName = "["+Thread.currentThread().getName()+"]";
    System.err.println(threadName+getClass().getSimpleName()+Arrays.asList(args));
  }

}

@Service
class DynamoInputPluginProvider implements InputPluginProvider {

  @Override
  public InputPlugin get(ApplicationArguments args) throws Exception {

    DynamoOptions options = OptionArgs.parseOptions(args, DynamoOptions.class);

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    if (options.rcuLimit == -1)
      options.rcuLimit = options.wcuLimit == -1 ? 128 : options.wcuLimit / 2;
    if (options.wcuLimit == -1)
      options.wcuLimit = options.rcuLimit * 8;

    if (options.totalSegments == 0)
      options.totalSegments = Math.max(options.rcuLimit/128, 1);

    log("options", options);

    String tableName = args.getNonOptionArgs().get(0);
    DynamoDbAsyncClient client = DynamoDbAsyncClient.create();
    DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
    DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();
    // describeTableResponse.table().
    return new DynamoInputPlugin(client, tableName, options.rcuLimit, options.totalSegments);
  }
  
  private void log(Object... args) {
    String threadName = "["+Thread.currentThread().getName()+"]";
    System.err.println(threadName+getClass().getSimpleName()+Arrays.asList(args));
  }

}
