package app;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoOutputPlugin implements OutputPlugin {

  private final DynamoDbAsyncClient client;
  private final String tableName;
  private final RateLimiter writeLimiter;
  private final BlockingQueue<Number> permitsQueue; // thundering herd

  private static ExecutorService executor = Executors.newCachedThreadPool();

  public DynamoOutputPlugin(DynamoDbAsyncClient client, String tableName, RateLimiter writeLimiter, BlockingQueue<Number> permitsQueue) {
    log("ctor", tableName);
    this.client = client;
    this.tableName = tableName;
    this.writeLimiter = writeLimiter;
    this.permitsQueue = permitsQueue;
  }

  @Override
  public ListenableFuture<?> write(Iterable<JsonElement> jsonElements) {
    // log("write", jsonElement);
    return new FutureRunner() {
      int num;
      List<ListenableFuture<?>> futures = new ArrayList<>();
      {
        run(() -> {
          // A single BatchWriteItem operation can contain up to 25 PutItem or DeleteItem requests.
          // The total size of all the items written cannot exceed 16 MB.
          for (Iterable<JsonElement> partition : Iterables.partition(jsonElements, 25)) {
            run(()->{
              List<WriteRequest> requestItems = new ArrayList<>();
              for (JsonElement jsonElement : partition) {
                Map<String, AttributeValue> item = render(jsonElement);
                PutRequest putRequest = PutRequest.builder().item(item).build();
                WriteRequest writeRequest = WriteRequest.builder().putRequest(putRequest).build();
                requestItems.add(writeRequest);
              }
              ListenableFuture<?> lf = flush(++num, requestItems);
              futures.add(lf);
              return lf;
            });
          }
          return Futures.immediateVoidFuture();
          // return Futures.successfulAsList(futures);
        });
      }
    }.get();
  }

      // @Override
      // public ListenableFuture<?> flush() {
      //   // log("flush");
      //   return new FutureRunner() {
      //     {
      //       run(() -> {
      //         ListenableFuture<?> lf = Futures.successfulAsList(partition.values());
      //         if (partition.size() > 0)
      //           partition = flush(partition);
      //         return lf;
      //       });
      //     }
      //   }.get();
      // }

  class FlushRecord {
    public boolean success;
    public String failureMessage;
    public String toString() {
      return new Gson().toJson(this);
    }
  }

  private ListenableFuture<?> flush(int num, List<WriteRequest> partition) {
    // log(num, "flush", partition.size());
    return new FutureRunner() {
      int consumedCapacityUnits;
      {
        run(()->{
          return Futures.submit(()->{
            // log(num, "take permits");
            Number permits = permitsQueue.take();
            // log(num, "permits taken ", permits);
            return permits;
          }, executor);
        }, permits->{

          run(() -> {

            // permits = number.intValue();

            // throttle
            // log(num, "acquire", permits);
            if (permits.intValue() > 0)
              writeLimiter.acquire(permits.intValue()); // consume permits
            // log(num, "acquired", permits);
  
            BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                //
                .requestItems(ImmutableMap.of(tableName, partition))
                //
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                //
                .build();
  
            return lf(client.batchWriteItem(batchWriteItemRequest));
            
          }, batchWriteItemResponse -> {
  
            for (ConsumedCapacity consumedCapacity : batchWriteItemResponse.consumedCapacity()) {
              consumedCapacityUnits += consumedCapacity.capacityUnits().intValue();
            }
  
            if (batchWriteItemResponse.hasUnprocessedItems()) {
              for (List<WriteRequest> unprocessedItems : batchWriteItemResponse.unprocessedItems().values()) {
                for (WriteRequest writeRequest : unprocessedItems) {
                  log("###unprocessedItem###", writeRequest);
                }
              }
            }
  
          }, e->{ // catch
            log(e);
            e.printStackTrace();
            throw new RuntimeException(e);
          }, ()->{ // finally
            // log(record);
            log(num, "flushed", partition.size());
            try {
              // log(num, "put permits", consumedCapacityUnits);
              permitsQueue.put(consumedCapacityUnits); // guaranteed produce permits
              // log(num, "permits put", consumedCapacityUnits);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
  
        });

      }
    }.get();
  }
  
  private Map<String, AttributeValue> render(JsonElement jsonElement) throws Exception {
    Map<String, AttributeValue> item = new HashMap<>();
    for (Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
      item.put(entry.getKey(), new ObjectMapper().readValue(entry.getValue().toString(), AttributeValue.serializableBuilderClass()).build());
    }
    return item;
  }

  private void log(Object... args) {
    String threadName = "["+Thread.currentThread().getName()+"]";
    System.err.println(threadName+getClass().getSimpleName()+Arrays.asList(args));
  }
}

@Service
class DynamoOutputPluginProvider implements OutputPluginProvider {

  @Override
  public Supplier<OutputPlugin> get(ApplicationArguments args) throws Exception {
    String tableName = args.getNonOptionArgs().get(1);

    DynamoOptions options = OptionArgs.parseOptions(args, DynamoOptions.class);

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    if (options.rcuLimit == -1)
      options.rcuLimit = options.wcuLimit == -1 ? 128 : options.wcuLimit / 2;
    if (options.wcuLimit == -1)
      options.wcuLimit = options.rcuLimit * 8;

    log("options", options);
  
    DynamoDbAsyncClient client = DynamoDbAsyncClient.create();
    DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
    DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();

    RateLimiter writeLimiter = RateLimiter.create(options.wcuLimit);

    // thundering herd
    //###TODO get some sort of inputPlugin concurrency hint here
    //###TODO get some sort of inputPlugin concurrency hint here
    //###TODO get some sort of inputPlugin concurrency hint here
    ArrayBlockingQueue<Number> permitsQueue = Queues.newArrayBlockingQueue(20); //###TODO get some sort of inputPlugin concurrency hint here
    while (permitsQueue.remainingCapacity()>0)
      permitsQueue.add(options.wcuLimit);
    //###TODO get some sort of inputPlugin concurrency hint here
    //###TODO get some sort of inputPlugin concurrency hint here
    //###TODO get some sort of inputPlugin concurrency hint here

    return ()->{
      return new DynamoOutputPlugin(client, tableName, writeLimiter, permitsQueue);
    };
  }

  private void log(Object... args) {
    String threadName = "["+Thread.currentThread().getName()+"]";
    System.err.println(threadName+getClass().getSimpleName()+Arrays.asList(args));
  }

}