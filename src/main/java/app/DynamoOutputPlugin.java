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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
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

  private final ExecutorService executor1;
  private final ExecutorService executor2;

    private final MetricRegistry metrics = new MetricRegistry();
  
      private Meter wcuMeter() { return metrics.meter("wcu"); }


  public DynamoOutputPlugin(DynamoDbAsyncClient client, String tableName, RateLimiter writeLimiter, BlockingQueue<Number> permitsQueue, ExecutorService executor1, ExecutorService executor2) {
    log("ctor", tableName);
    this.client = client;
    this.tableName = tableName;
    this.writeLimiter = writeLimiter;
    this.permitsQueue = permitsQueue;
    this.executor1 = executor1;
    this.executor2 = executor2;
  }

  @Override
  public ListenableFuture<?> write(Iterable<JsonElement> jsonElements) {
    // log("write", jsonElement);
    return new FutureRunner() {
      int num;
      {
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
            ListenableFuture<?> lf = write(++num, requestItems);
            // futures.add(lf);
            return lf;
          });
        }
      }
    }.get();
  }

    class WriteRecord {
      public boolean success;
      public String failureMessage;
      public int unprocessedItems;
      public int consumedCapacityUnits;
      public int wcuMeanRate;
      public String toString() {
        return getClass().getSimpleName()+new Gson().toJson(this);
      }
    }

  private ListenableFuture<?> write(int num, List<WriteRequest> partition) {
    // log(num, "write", partition.size());
    return new FutureRunner() {
      WriteRecord record = new WriteRecord();
      {
        run(()->{

          return Futures.submit(()->{
            return permitsQueue.take();
          }, executor1);

        }, permits ->{

          run(()->{
            return Futures.submit(()->{
              try {
              // log(num, "acquire", permits);
              if (permits.intValue() > 0)
                  writeLimiter.acquire(permits.intValue()); // consume permits
              // log(num, "acquired", permits);
            } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }, executor1);
  
          }, result->{

            run(() -> {

              // permits = number.intValue();
  
              // throttle
              // log(num, "acquire", permits);
                        // if (permits.intValue() > 0)
                        //   writeLimiter.acquire(permits.intValue()); // consume permits



                        // writeLimiter.acquire(250); // consume permits


    
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
                wcuMeter().mark(consumedCapacity.capacityUnits().longValue());
                record.consumedCapacityUnits += consumedCapacity.capacityUnits().intValue();
              }
                  // if (consumedCapacityUnits > 0)
                  //   writeLimiter.acquire(consumedCapacityUnits); // consume permits
    
              if (batchWriteItemResponse.hasUnprocessedItems()) {
                if (!batchWriteItemResponse.unprocessedItems().isEmpty()) {
                  int size = batchWriteItemResponse.unprocessedItems().values().iterator().next().size();
                  if (size>0) {
                    record.unprocessedItems = size;
                    // log("unprocessedItems", size);
                  }
                }
                // log("unprocessedItems", batchWriteItemResponse.unprocessedItems());
                // for (List<WriteRequest> unprocessedItems : batchWriteItemResponse.unprocessedItems().values()) {
                //   for (WriteRequest writeRequest : unprocessedItems) {
                //     log("###unprocessedItem###", writeRequest);
                //   }
                // }
              }

              record.success = true;

            }, e->{ // catch
              record.failureMessage = ""+e;
            }, ()->{ // finally
              // log(record);
              record.wcuMeanRate = Double.valueOf(wcuMeter().getMeanRate()).intValue();
              // log(num, "write", partition.size(), wcuMeter().getMeanRate());
              try {
                // log(num, "put permits", consumedCapacityUnits);
                permitsQueue.put(record.consumedCapacityUnits); // guaranteed produce permits
                // log(num, "permits put", consumedCapacityUnits);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    
          });
  
        // }, e->{
        //   log(e);
        //   e.printStackTrace();
        //   throw new RuntimeException(e);
        // }, ()->{

        });

      }
      @Override
      protected void onListen() {
        log(record);
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
  public OutputPlugin get(ApplicationArguments args) throws Exception {
    String tableName = args.getNonOptionArgs().get(1);

    DynamoOptions options = OptionArgs.parseOptions(args, DynamoOptions.class);

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    if (options.rcuLimit == -1)
      options.rcuLimit = options.wcuLimit == -1 ? 128 : options.wcuLimit / 2;
    if (options.wcuLimit == -1)
      options.wcuLimit = options.rcuLimit * 8;

    if (options.totalSegments == 0)
      options.totalSegments = Math.max(options.rcuLimit/128, 1);

    log("options", options);
  
    DynamoDbAsyncClient client = DynamoDbAsyncClient.create();
    DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
    DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();

    RateLimiter writeLimiter = RateLimiter.create(options.wcuLimit);
    log("writeLimiter", writeLimiter);

    writeLimiter.acquire(options.wcuLimit);
    writeLimiter.acquire(options.wcuLimit);

    ExecutorService executor1 = Executors.newCachedThreadPool();
     ExecutorService executor2 = Executors.newCachedThreadPool();
    //  ExecutorService executor1 = Executors.newFixedThreadPool(options.totalSegments);
    // ExecutorService executor2 = Executors.newFixedThreadPool(options.totalSegments);

    // thundering herd
    //###TODO get some sort of inputPlugin concurrency hint here
    //###TODO get some sort of inputPlugin concurrency hint here
    //###TODO get some sort of inputPlugin concurrency hint here
    ArrayBlockingQueue<Number> permitsQueue = Queues.newArrayBlockingQueue(options.totalSegments); //###TODO get some sort of inputPlugin concurrency hint here
    for (int i = 0; i < options.totalSegments; ++i)
      permitsQueue.add(options.wcuLimit); // stagger?
    //###TODO get some sort of inputPlugin concurrency hint here
    //###TODO get some sort of inputPlugin concurrency hint here
    //###TODO get some sort of inputPlugin concurrency hint here

    return new DynamoOutputPlugin(client, tableName, writeLimiter, permitsQueue, executor1, executor2);
  }

  private void log(Object... args) {
    String threadName = "["+Thread.currentThread().getName()+"]";
    System.err.println(threadName+getClass().getSimpleName()+Arrays.asList(args));
  }

}