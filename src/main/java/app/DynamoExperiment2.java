package app;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Joiner;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.spotify.futures.CompletableFuturesExtra;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

class MyRecord {
  public boolean success;
  public String failureMessage;
  public double rateIn; // desired
  public double instantRate; // reported
  public double fastRate; // reported
  public double slowRate; // reported
  public String meter;
  public double rateOut;// desired

  public String toString() {
    return gson.toJson(this);
  }

  static Gson gson = new GsonBuilder()
      .registerTypeAdapter(Double.class, (JsonSerializer<Double>) (src, typeOfSrc, context) -> {
        DecimalFormat df = new DecimalFormat("#.#");
        df.setRoundingMode(RoundingMode.HALF_EVEN);
        return new JsonPrimitive(Double.parseDouble(df.format(src)));
      }).create();
}

public class DynamoExperiment2 {

  static AtomicLong id = new AtomicLong();

  static {
    ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("ROOT")).setLevel(ch.qos.logback.classic.Level.INFO);
  }

  static DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().httpClient(AwsSdkTwo.httpClient).build();
  static String tableName = "MyTable";

  static int concurrency = 4;
  static Semaphore sem = new Semaphore(concurrency);

  static long fastWindow = 6;
  static long slowWindow = 30;

  static RateLimiter rateLimiter = RateLimiter.create(5.0);
  static LocalMeter reportedMeter = new LocalMeter();
  // static LocalMeter secondMeter = new LocalMeter();

  static final Object lock = new Object();


  public static void main(String... args) throws Exception {

    //###TODO ERADICATE ME
    //###TODO ERADICATE ME
    //###TODO ERADICATE ME
    rateLimiter.acquire(Double.valueOf(rateLimiter.getRate()).intValue());
    //###TODO ERADICATE ME
    //###TODO ERADICATE ME
    //###TODO ERADICATE ME

    // prime client
    log(client.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).get());

    while (true) {

      var myRecord = new MyRecord();

      sem.acquire();

      var t0 = System.currentTimeMillis();

      try {
        String key = Hashing.sha256().hashLong(id.incrementAndGet()).toString();
        
        var item = new HashMap<String, AttributeValue>();
        item.put("id", AttributeValue.builder().s(key).build());
        item.put("val1", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
        item.put("val2", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
        item.put("val3", AttributeValue.builder().s(UUID.randomUUID().toString()).build());

        var putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        var lf = lf(client.putItem(putItemRequest));

        lf.addListener(()->{
          synchronized (lock) {
          try {
            var t = System.currentTimeMillis();

            var putItemResponse = lf.get();
            var consumedCapacityUnits = putItemResponse.consumedCapacity().capacityUnits();

            // real -> adjusted
            // nominal -> unadjusted

            var instantRate = Double.valueOf(1000.0 * consumedCapacityUnits / (t - t0) * concurrency).doubleValue();
            myRecord.instantRate = instantRate;

            //THROTTLE
            rateLimiter.acquire(consumedCapacityUnits.intValue());
            //THROTTLE

            reportedMeter.mark(consumedCapacityUnits.longValue());

            // 80.0 -> 0.5    0.0/0.0

            var fastRate = reportedMeter.avg(fastWindow).doubleValue();
            myRecord.fastRate = reportedMeter.avg(fastWindow).doubleValue();

            var slowRate = reportedMeter.avg(slowWindow).doubleValue();
            myRecord.slowRate = reportedMeter.avg(slowWindow).doubleValue();

            myRecord.rateIn = rateLimiter.getRate();
            // log("rate", rate, "delta", delta);

            // double factor = 5; // how aggressive

            // speeding up?
            if (fastRate > slowRate) {
              
              double factor = 2; // how aggressive

              // we desire to achieve instantRate or fastRate
              // the gap to instantRate is... from where we are now to instantRate

              // "rateIn":5.0,"instantRate":56.737588652482266,"fastRate":0.2,"slowRate":0.1,"rateOut":30.868794326241133}
              // "rateIn":30.868794326241133,"instantRate":36.03603603603604,"fastRate":0.4,"slowRate":0.2,"rateOut":33.45241518113858}

              var rateIn = rateLimiter.getRate();
              var desiredRate = Math.max(instantRate, fastRate); //###TODO PONDER THIS
              // var whereWeAreNow = rateIn; // Math.max(rateIn, fastRate); //###TODO SHOULD THIS BE JUST RATEIN ?? 
              var desiredDelta = desiredRate - rateIn;

                  // var creepUp=(fastRate-slowRate)/slowRate;
                  // log(creepUp);
              var rateOut = rateIn + desiredDelta * .02; // creep up slow
              // var rateOut = rateIn + desiredDelta*slowRate/fastRate;

              myRecord.rateOut = rateOut;

              rateLimiter.setRate(rateOut);

            }

            //.IllegalArgumentException: rate must be positive","rateIn":10.2,"instantRate":0.6476683937823834,"fastRate":0.0,"slowRate":4.6,"rateOut":0.0}

            // slowing down?
            if (fastRate < slowRate) {

              double factor = 3/2; // how aggressive

              // we desire to achieve instantRate or fastRate
              // the gap to fastRate is... from where we are now to fastRate

              // "rateIn":85.41694762895051,"instantRate":89.88764044943821,"fastRate":17.2,"slowRate":42.4,"rateOut":88.07405751282528}
              // "rateIn":45.958635258974546,"instantRate":47.61904761904762,"fastRate":47.2,"slowRate":59.4,"rateOut":46.29966271340033}

                    // var absDesire = Math.min(instantRate, fastRate);
              
              var rateIn = rateLimiter.getRate();

              var desiredRate = slowRate; //###TODO PONDER THIS
              // var desiredRate = Math.min(instantRate, slowRate);

                    // var desiredRate = instantRate; // // want lowest non-zero
                    // if (fastRate > 0) {
                    //   if (fastRate < instantRate)
                    //     desiredRate = fastRate;
                    // }

              // var whereWeAreNow = rateIn; // Math.min(rateIn, fastRate);
              var desiredDelta = rateIn - desiredRate;
              var rateOut = rateIn - desiredDelta * 0.50; // thunk down fast
              // var rateOut = rateIn - desiredDelta*.5 + desiredDelta*fastRate/slowRate / factor;

              myRecord.rateOut = rateOut;

              rateLimiter.setRate(rateOut);

            }

            myRecord.success=true;

          } catch (Exception e) {
            myRecord.failureMessage = ""+e;
          } finally {
            sem.release();
            myRecord.meter = reportedMeter.toString();
            log(myRecord);
          }
        } // synchronized
        }, MoreExecutors.directExecutor());

      } catch (Exception e) {
        log(e);
      } finally {
      }
    }
  }

  // convenience
  static <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
    return CompletableFuturesExtra.toListenableFuture(cf);
  }

  static void log(Object... args) {    
    System.out.println(Instant.now().toString()+" "+Joiner.on(" ").useForNull("null").join(args));
  }

}
