package helpers;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoWriter {

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
  private final Iterable<String> keySchema;
  private final boolean delete; // PutItem vs DeleteItem
  private final RateLimiter writeLimiter;

  // ###TODO
  // ###TODO
  // ###TODO
  private final Semaphore sem = new Semaphore(Runtime.getRuntime().availableProcessors());
  // ###TODO
  // ###TODO
  // ###TODO

  private Multimap<WriteRequest, VoidFuture> partition = ArrayListMultimap.create();
  private final List<ListenableFuture<?>> batchWriteItemFutures = Lists.newCopyOnWriteArrayList();

  // ###TODO
  // ###TODO
  // ###TODO
  private static LocalMeter wcuMeter = new LocalMeter();
  // ###TODO
  // ###TODO
  // ###TODO

  public DynamoWriter(DynamoDbAsyncClient client, String tableName, Iterable<String> keySchema, boolean delete, RateLimiter writeLimiter) {
    debug("ctor", client, tableName, keySchema, delete, writeLimiter);
    this.client = client;
    this.tableName = tableName;
    this.keySchema = keySchema;
    this.delete = delete;
    this.writeLimiter = writeLimiter;
  }

  public ListenableFuture<?> write(JsonElement jsonElement) {
    return new FutureRunner() {
      {
        run(() -> {

          Map<String, AttributeValue> item = render(jsonElement);
          PutRequest putRequest = PutRequest.builder().item(item).build();
          WriteRequest writeRequest = WriteRequest.builder().putRequest(putRequest).build();
          if (delete) {
            Map<String, AttributeValue> key = Maps.toMap(keySchema, k -> item.get(k));
            DeleteRequest deleteRequest = DeleteRequest.builder().key(key).build();
            writeRequest = WriteRequest.builder().deleteRequest(deleteRequest).build();
          }

          VoidFuture lf = new VoidFuture();
          partition.put(writeRequest, lf);

          // A single BatchWriteItem operation can contain up to 25 PutItem or DeleteItem
          // requests. The total size of all the items written cannot exceed 16 MB.

          if (partition.size() == 25) //###TODO use .keySet().size() ??
            partition = doBatchWriteItem(partition);

          return lf;
        });
      }
    }.get();
  }

  public ListenableFuture<?> flush() {
    return new FutureRunner() {
      {
        run(() -> {
          if (partition.size() > 0) //###TODO use .keySet().size() ??
            partition = doBatchWriteItem(partition);
          return Futures.successfulAsList(batchWriteItemFutures);
        });
      }
    }.get();
  }

  class DoBatchWriteItemWork {
    public boolean success;
    public String failureMessage;
    public int unprocessedItems;
    public int consumedCapacityUnits;
    public String wcuMeter;
    public String toString() {
      return "DoBatchWriteItemWork" + new Gson().toJson(this);
    }
  }

  private Multimap<WriteRequest, VoidFuture> doBatchWriteItem(Multimap<WriteRequest, VoidFuture> partition) {

    //###TODO use semaphore??

    // run(()->{
    //   sem.acquire();
    //   List<WriteRequest> requestItems = new ArrayList<>();
    //   for (JsonElement jsonElement : partition)
    //   {
    //     Map<String, AttributeValue> item = render(jsonElement);
    //     PutRequest putRequest = PutRequest.builder().item(item).build();
    //     WriteRequest writeRequest = WriteRequest.builder().putRequest(putRequest).build();
    //     if (delete) {
    //       Map<String, AttributeValue> key = Maps.toMap(keySchema, k->item.get(k));
    //       DeleteRequest deleteRequest = DeleteRequest.builder().key(key).build();
    //       writeRequest = WriteRequest.builder().deleteRequest(deleteRequest).build();
    //     }
    //     requestItems.add(writeRequest);
    //   }
    //   batchWriteItem(++num, requestItems).addListener(()->{
    //     sem.release();
    //   }, MoreExecutors.directExecutor());
    // });

    var lf = new FutureRunner() {
      DoBatchWriteItemWork work = new DoBatchWriteItemWork();
      {
        run(() -> {

          BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
              //
              .requestItems(ImmutableMap.of(tableName, partition.keySet()))
              //
              .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
              //
              .build();

          return lf(client.batchWriteItem(batchWriteItemRequest));

        }, batchWriteItemResponse -> {

          for (ConsumedCapacity consumedCapacity : batchWriteItemResponse.consumedCapacity()) {
            wcuMeter.add(consumedCapacity.capacityUnits().longValue());
            work.consumedCapacityUnits += consumedCapacity.capacityUnits().intValue();
            int permits = consumedCapacity.capacityUnits().intValue();
            if (permits > 0)
              writeLimiter.acquire(permits);
          }

          // failure 500
          if (batchWriteItemResponse.hasUnprocessedItems()) {
            for (List<WriteRequest> unprocessedItems : batchWriteItemResponse.unprocessedItems().values()) {

              //###TODO re-submit unprocessedItems
              //###TODO re-submit unprocessedItems
              //###TODO re-submit unprocessedItems
              // doWrite(batchWriteItemResponse.unprocessedItems().values().iterator().next());
              //###TODO re-submit unprocessedItems
              //###TODO re-submit unprocessedItems
              //###TODO re-submit unprocessedItems

              for (WriteRequest writeRequest : unprocessedItems) {
                partition.removeAll(writeRequest).forEach(lf -> {
                  ++work.unprocessedItems;
                  lf.setException(new Exception("unprocessedItem"));
                });
              }
            }
          }

          // success 200
          partition.values().forEach(lf -> {
            lf.setVoid();
          });

          work.success = true;

        }, e -> {
          debug(e);
          e.printStackTrace();
          work.failureMessage = "" + e;
          partition.values().forEach(lf -> {
            lf.setException(e);
          });
          // throw e;
        }, () -> {
          // finally
          work.wcuMeter = wcuMeter.toString();
        });
      }

      @Override
      protected void onListen() {
        debug(work);
      }
    }.get();
    batchWriteItemFutures.add(lf);
    return ArrayListMultimap.create();
  }

  private Map<String, AttributeValue> render(JsonElement jsonElement) throws Exception {
    var item = new LinkedHashMap<String, AttributeValue>();
    for (var entry : jsonElement.getAsJsonObject().entrySet()) {
      var key = entry.getKey();
      var attributeValue = new ObjectMapper().readValue(entry.getValue().toString(), AttributeValue.serializableBuilderClass()).build();
      item.put(key, attributeValue);
    }
    return item;
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }

}
