package app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
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

public class InputPluginDynamo implements InputPlugin {

  private final DynamoDbAsyncClient client;
  private final String tableName;
  private final int totalSegments;
  private final RateLimiter readLimiter;

  public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList();

  private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

  // thundering herd
  private final BlockingQueue<Number> permitsQueue = Queues.newArrayBlockingQueue(1);

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

    permitsQueue.add(rcuLimit);

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    exclusiveStartKeys.addAll(Collections.nCopies(Math.max(rcuLimit/128, 1), null));

  }

  @Override
  public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
    this.listener = listener;
  }

  @Override
  public ListenableFuture<?> read() throws Exception {
    return new FutureRunner() {
      // int permits; //###TODO THIS IS NOT RIGHT BEING HERE
      // int permits; //###TODO THIS IS NOT RIGHT BEING HERE
      // int permits; //###TODO THIS IS NOT RIGHT BEING HERE
      int permits; //###TODO THIS IS NOT RIGHT BEING HERE
      // int permits; //###TODO THIS IS NOT RIGHT BEING HERE
      // int permits; //###TODO THIS IS NOT RIGHT BEING HERE
      // int permits; //###TODO THIS IS NOT RIGHT BEING HERE
      {
        for (int segment = 0; segment < totalSegments; ++segment)
          doSegment(segment);
      }
      void doSegment(int segment) {
        List<JsonElement> list = new ArrayList<>();
        run(() -> {

          log("permitsQueue.take");
          int permits = permitsQueue.take().intValue();
          log("permits:"+permits);

          log("acquire");
          if (permits > 0)
            readLimiter.acquire(permits);
          log("acquire done");

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
              // .limit(250)
              //
              .build();

          log(scanRequest);

          return lf(client.scan(scanRequest));
        }, scanResponse -> {

          log(scanResponse.items().size());

          exclusiveStartKeys.set(segment, scanResponse.lastEvaluatedKey());

          permits = scanResponse.consumedCapacity().capacityUnits().intValue();

          // STEP 2 Process results here

          // readCount.addAndGet(scanResponse.items().size());

          for (Map<String, AttributeValue> item : scanResponse.items()) {
            list.add(parse(item));
            // System.out.println(render(item));
          }

          // run(()->{
          //   return listener.apply(list);
          // }, ()->{ // finally
          //   if (!exclusiveStartKeys.get(segment).isEmpty())
          //     doSegment(segment);
          // });

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
        }, ()->{
          try {
            permitsQueue.put(permits); // replenish permits
          } catch (Exception e) {
            log(e);
            e.printStackTrace();
            throw new RuntimeException(e);
          } finally {

            run(()->{
              return listener.apply(list);
            }, ()->{ // finally
              if (!exclusiveStartKeys.get(segment).isEmpty())
                doSegment(segment); // consumes permits
            });
  
          }
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
    System.err.println(getClass().getSimpleName()+Lists.newArrayList(args));
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
