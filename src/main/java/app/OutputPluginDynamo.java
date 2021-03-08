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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
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

public class OutputPluginDynamo implements OutputPlugin {

  private class VoidFuture extends AbstractFuture<Void> {
    public boolean setVoid() {
        return super.set(null);
    }

    public boolean setException(Throwable throwable) {
        return super.setException(throwable);
    }
  }

  private final DynamoDbAsyncClient client;
  private final String tableName;
  private final int wcuLimit;
  private final RateLimiter writeLimiter;

  private Multimap<WriteRequest, VoidFuture> partition = ArrayListMultimap.create();

  // thundering herd
  private final BlockingQueue<Number> permitsQueue = Queues.newArrayBlockingQueue(1);

  public OutputPluginDynamo(DynamoDbAsyncClient client, String tableName, int wcuLimit) {
    log("ctor", client, tableName, wcuLimit);
    this.client = client;
    this.tableName = tableName;
    this.wcuLimit = wcuLimit;

    writeLimiter = RateLimiter.create(wcuLimit);

    permitsQueue.add(wcuLimit);

  }

  @Override
  public ListenableFuture<?> write(JsonElement jsonElement) {
    // log("write", jsonElement);
    return new FutureRunner() {
      {
        run(() -> {
          if (partition.size() == 25)
             partition = flush(partition);

          Map<String, AttributeValue> item = render(jsonElement);
          WriteRequest writeRequest = WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build();

          VoidFuture lf = new VoidFuture();
          partition.put(writeRequest, lf);
          return lf;
        });
      }
    }.get();
  }

  @Override
  public ListenableFuture<?> flush() {
    // log("flush");
    return new FutureRunner() {
      {
        run(() -> {
          ListenableFuture<?> result = Futures.successfulAsList(partition.values());
          if (partition.size() > 0)
            partition = flush(partition);
          return result;
        });
      }
    }.get();
  }

  class FlushRecord {
    public boolean success;
    public String failureMessage;
    public String toString() {
      return new Gson().toJson(this);
    }
  }

  private Multimap<WriteRequest, VoidFuture> flush(Multimap<WriteRequest, VoidFuture> partition) {
    log("flush", partition.size());
    new FutureRunner() { //###TODO REALLY FIRE/FORGET ?!?
      int permits;
      {
        run(() -> {
          
          // throttle
          int permits = permitsQueue.take().intValue();
          if (permits > 0)
            writeLimiter.acquire(permits);

          BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
              //
              .requestItems(ImmutableMap.of(tableName, partition.keySet()))
              //
              .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
              //
              .build();

          return lf(client.batchWriteItem(batchWriteItemRequest));
          
        }, batchWriteItemResponse -> {

          try {

            for (ConsumedCapacity consumedCapacity : batchWriteItemResponse.consumedCapacity()) {
              permits += consumedCapacity.capacityUnits().intValue();
              // writeLimiter.acquire(consumedCapacity.capacityUnits().intValue());
            }
  
              // failure 500
            if (batchWriteItemResponse.hasUnprocessedItems()) {
              for (List<WriteRequest> unprocessedItems : batchWriteItemResponse.unprocessedItems().values()) {
                for (WriteRequest writeRequest : unprocessedItems) {
                  partition.removeAll(writeRequest).forEach(lf -> {
                    lf.setException(new Exception("unprocessedItem"));
                  });
                }
              }
            }
            // success 200
            partition.values().forEach(lf -> {
              lf.setVoid();
            });
  
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

        }, e->{
          log(e);
          e.printStackTrace();
          partition.values().forEach(lf -> {
            lf.setException(e);
          });
        }, ()->{
          // log(record);
          try {
            permitsQueue.put(permits);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
    };
    return ArrayListMultimap.create();
  }
  
  private Map<String, AttributeValue> render(JsonElement jsonElement) throws Exception {
    Map<String, AttributeValue> item = new HashMap<>();
    for (Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
      item.put(entry.getKey(), new ObjectMapper().readValue(entry.getValue().toString(), AttributeValue.serializableBuilderClass()).build());
    }
    return item;
  }

  private void log(Object... args) {
    System.err.println(getClass().getSimpleName()+Lists.newArrayList(args));
  }
}

@Service
class OutputPluginProviderDynamo implements OutputPluginProvider {

  @Override
  public OutputPlugin get(ApplicationArguments args) throws Exception {
    String tableName = args.getNonOptionArgs().get(1);
    DynamoDbAsyncClient client = DynamoDbAsyncClient.create();
    DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
    DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest).get();
    // describeTableResponse.table().
    return new OutputPluginDynamo(client, tableName, 128);
  }

  // private void log(Object... args) {
  //   System.err.println(getClass().getSimpleName()+Lists.newArrayList(args));
  // }

}