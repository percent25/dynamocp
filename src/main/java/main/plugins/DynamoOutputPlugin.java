package main.plugins;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import helpers.FutureRunner;
import helpers.LocalMeter;
import helpers.LogHelper;
import main.Args;
import main.Options;
import main.OutputPlugin;
import main.OutputPluginProvider;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoOutputPlugin implements OutputPlugin {

  private final DynamoDbAsyncClient client;
  private final String tableName;
  private final RateLimiter writeLimiter;
  // private final BlockingQueue<Number> permitsQueue; // thundering herd
  private final boolean delete; // if true then issue DeleteItems (vs PutItems)
  private final Iterable<String> keySchema;

  //###TODO
  //###TODO
  //###TODO
  private final Semaphore sem = new Semaphore(Runtime.getRuntime().availableProcessors());
  //###TODO
  //###TODO
  //###TODO

  private static LocalMeter wcuMeter = new LocalMeter();

  public DynamoOutputPlugin(DynamoDbAsyncClient client, String tableName, RateLimiter writeLimiter, BlockingQueue<Number> permitsQueue, boolean delete, Iterable<String> keySchema) {
    log("ctor", tableName);
    this.client = client;
    this.tableName = tableName;
    this.writeLimiter = writeLimiter;
    this.delete = delete;
    this.keySchema = keySchema;
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

          // pre-throttle
            // writeLimiter.acquire(25);

          run(()->{

            sem.acquire();

            List<WriteRequest> requestItems = new ArrayList<>();
            for (JsonElement jsonElement : partition) {
              Map<String, AttributeValue> item = render(jsonElement);
              PutRequest putRequest = PutRequest.builder().item(item).build();
              WriteRequest writeRequest = WriteRequest.builder().putRequest(putRequest).build();
              if (delete) {
                Map<String, AttributeValue> key = Maps.toMap(keySchema, k->item.get(k));
                DeleteRequest deleteRequest = DeleteRequest.builder().key(key).build();
                writeRequest = WriteRequest.builder().deleteRequest(deleteRequest).build();
              }
              requestItems.add(writeRequest);
            }
            batchWriteItem(++num, requestItems).addListener(()->{
              sem.release();
            }, MoreExecutors.directExecutor());

            //###TODO RETURN BATCHWRITEITEM LF HERE??
            //###TODO RETURN BATCHWRITEITEM LF HERE??
            //###TODO RETURN BATCHWRITEITEM LF HERE??
            return Futures.immediateVoidFuture();
            //###TODO RETURN BATCHWRITEITEM LF HERE??
            //###TODO RETURN BATCHWRITEITEM LF HERE??
            //###TODO RETURN BATCHWRITEITEM LF HERE??

          });
        }
      }
    }.get();
  }

    class BatchWriteItemRecord {
      public boolean success;
      public String failureMessage;
      public int unprocessedItems;
      public int consumedCapacityUnits;
      public String wcuMeter;
      public String toString() {
        return "BatchWriteItemRecord"+new Gson().toJson(this);
      }
    }

  private ListenableFuture<?> batchWriteItem(int num, Collection<WriteRequest> requestItems) {
    // log(num, "write", partition.size());
    return new FutureRunner() {
      BatchWriteItemRecord record = new BatchWriteItemRecord();
      {
        doWrite(requestItems);
      }
      void doWrite(Collection<WriteRequest> requestItems) {
        run(() -> {

          BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
              //
              .requestItems(ImmutableMap.of(tableName, requestItems))
              //
              .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
              //
              .build();

          return lf(client.batchWriteItem(batchWriteItemRequest));
          
        }, batchWriteItemResponse -> {

          for (ConsumedCapacity consumedCapacity : batchWriteItemResponse.consumedCapacity()) {
            wcuMeter.mark(consumedCapacity.capacityUnits().longValue());
            record.consumedCapacityUnits += consumedCapacity.capacityUnits().intValue();
          }

          // post-throttle
              if (record.consumedCapacityUnits > 0)
                writeLimiter.acquire(record.consumedCapacityUnits); // consume permits

          if (batchWriteItemResponse.hasUnprocessedItems()) {
            if (!batchWriteItemResponse.unprocessedItems().isEmpty()) {
                  int size = batchWriteItemResponse.unprocessedItems().values().iterator().next().size();
                  record.unprocessedItems += size;
                  doWrite(batchWriteItemResponse.unprocessedItems().values().iterator().next());
                  // if (size>0) {
                  //   record.unprocessedItems = size;
                  //   // log("unprocessedItems", size);
                  // }
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
          record.wcuMeter = wcuMeter.toString();
          // log(num, "write", partition.size(), wcuMeter().getMeanRate());
          try {
            // log(num, "put permits", consumedCapacityUnits);
                                    // permitsQueue.put(record.consumedCapacityUnits); // guaranteed produce permits
            // log(num, "permits put", consumedCapacityUnits);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
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
    new LogHelper(this).log(args);
  }
}

@Service
class DynamoOutputPluginProvider implements OutputPluginProvider {

  @Override
  public Supplier<OutputPlugin> get(String arg, ApplicationArguments args) throws Exception {
    if (arg.startsWith("dynamo:")) {
      String tableName = Args.parseArg(arg).split(":")[1];

      DynamoOptions options = Options.parse(arg, DynamoOptions.class);
      log("desired", options);
    
      DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().build();
      DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
      DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();

      Iterable<String> keySchema = Lists.transform(describeTableResponse.table().keySchema(), e->e.attributeName());      

      int provisionedRcu = describeTableResponse.table().provisionedThroughput().readCapacityUnits().intValue();
      int provisionedWcu = describeTableResponse.table().provisionedThroughput().writeCapacityUnits().intValue();

      options.infer(Runtime.getRuntime().availableProcessors(), provisionedRcu, provisionedWcu);
      log("reported", options);

      RateLimiter writeLimiter = RateLimiter.create(options.wcu == 0 ? Integer.MAX_VALUE : options.wcu);
      log("writeLimiter", writeLimiter);
    
      // thundering herd
      //###TODO get some sort of inputPlugin concurrency hint here
      //###TODO get some sort of inputPlugin concurrency hint here
      //###TODO get some sort of inputPlugin concurrency hint here
      ArrayBlockingQueue<Number> permitsQueue = Queues.newArrayBlockingQueue(options.totalSegments()); //###TODO get some sort of inputPlugin concurrency hint here
      while (permitsQueue.remainingCapacity()>0)
        permitsQueue.add(options.wcu); // stagger to work around thundering herd problem?
      //###TODO get some sort of inputPlugin concurrency hint here
      //###TODO get some sort of inputPlugin concurrency hint here
      //###TODO get some sort of inputPlugin concurrency hint here
  
      return () -> new DynamoOutputPlugin(client, tableName, writeLimiter, permitsQueue, options.delete, keySchema);
    }
    return null;
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}