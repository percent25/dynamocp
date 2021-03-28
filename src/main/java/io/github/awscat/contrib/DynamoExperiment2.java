package io.github.awscat.contrib;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

import helpers.LocalMeter;
import software.amazon.awssdk.metrics.LoggingMetricPublisher;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.MetricRecord;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

class MyRecord {
  public boolean success;
  public String failureMessage;
  public double rateIn; // desired
  public double instantRate; // reported
  public double fastRate; // reported
  public double slowRate; // reported
  public String dir;
  public String meter;
  public double rateOut;// desired

  public String toString() {
    return gson.toJson(this);
  }

  static Gson gson = new GsonBuilder()
  //
  .serializeSpecialFloatingPointValues()
      //
      // .registerTypeAdapter(Double.class, (JsonSerializer<Double>) (src, typeOfSrc, context) -> {
      //   DecimalFormat df = new DecimalFormat("#.#");
      //   df.setRoundingMode(RoundingMode.HALF_EVEN);
      //   return new JsonPrimitive(Double.parseDouble(df.format(src)));
      // })
      //
      .create();
}

/**
 * DynamoExperiment2
 */
public class DynamoExperiment2 {

  static int MINRATE=2;

  static AtomicLong id = new AtomicLong();

  static {
    ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("ROOT")).setLevel(ch.qos.logback.classic.Level.INFO);
  }

  static DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().build();
  static String tableName = "MyTable";

  static int concurrency = 2;
  static Semaphore sem = new Semaphore(concurrency);

  static long fastWindow = 2;
  static long slowWindow = 3;

  static {
    // assert 
  }

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

      MyRecord myRecord = new MyRecord();

      sem.acquire();

      // final var t0 = System.currentTimeMillis();

      try {
        String key = Hashing.sha256().hashLong(id.incrementAndGet()).toString();
        
        Map<String, AttributeValue> item = new LinkedHashMap<String, AttributeValue>();
        item.put("id", AttributeValue.builder().s(key).build());
        item.put("val1", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
        item.put("val2", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
        item.put("val3", AttributeValue.builder().s(UUID.randomUUID().toString()).build());

        int[] retryCount = new int[1];
        long[] serviceCallDuration = new long[1];
        PutItemRequest putItemRequest = PutItemRequest.builder()
        //
        .tableName(tableName).item(item).returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/metrics.html
        .overrideConfiguration(c->c.addMetricPublisher(new MetricPublisher(){

          @Override
          public void publish(MetricCollection metricCollection) {
            for (MetricRecord<?> metricRecord : extractAllMetrics(metricCollection)) {
              SdkMetric<?> sdkMetric = metricRecord.metric();
              Object value = metricRecord.value();
              if ("RetryCount".equals(sdkMetric.name()))
                retryCount[0]=Number.class.cast(value).intValue();
              if ("ServiceCallDuration".equals(sdkMetric.name()))
                serviceCallDuration[0]=Duration.class.cast(value).toMillis();
            }
          }
          @Override
          public void close() {
          }
          
        }))
        //
        .build();

        ListenableFuture<PutItemResponse> lf = lf(client.putItem(putItemRequest));

        lf.addListener(()->{
          synchronized (lock) {
          try {
            final long t0 = System.currentTimeMillis();

            PutItemResponse putItemResponse = lf.get();
            Double consumedCapacityUnits = putItemResponse.consumedCapacity().capacityUnits();

            double instantRate = Double.valueOf(1000.0 * consumedCapacityUnits / serviceCallDuration[0] * concurrency).doubleValue();
            myRecord.instantRate = instantRate;

            reportedMeter.add(consumedCapacityUnits.longValue());

            double fastRate = reportedMeter.avg(fastWindow).doubleValue();
            myRecord.fastRate = reportedMeter.avg(fastWindow).doubleValue();

            double slowRate = reportedMeter.avg(slowWindow).doubleValue();
            myRecord.slowRate = reportedMeter.avg(slowWindow).doubleValue();

            myRecord.rateIn = rateLimiter.getRate();
            // log("rate", rate, "delta", delta);

            if (slowRate <= fastRate) { // trending up

              myRecord.dir="UP";

              // double multiplier = 0; // how aggressive

              //"rateIn":5.0,"instantRate":46.8,"fastRate":0.1,"slowRate":0.06,"rateOut":25.9}

              double rateIn = rateLimiter.getRate();
              // var maxRate = Math.max(rateIn, instantRate); //###TODO PONDER THIS
              double ratio = 3/2 * (fastRate-slowRate)/slowRate; // (to-from)/from
              
              // var deltaRate = maxRate - rateIn;

                  // if (fastRate>slowRate) {
                  //   multiplier = (fastRate-slowRate)/slowRate;
                  //   if (multiplier>1)
                  //     multiplier=1;
                  // }

              // var rateOut = rateIn + deltaRate * multiplier; // creep up slowly
              // var rateOut = rateIn + desiredDelta*slowRate/fastRate;
              double rateOut = rateIn + ratio*rateIn;

              if (rateOut>instantRate)
                rateOut=instantRate;
              if (rateOut < MINRATE)
                rateOut = MINRATE;

              myRecord.rateOut = rateOut;

              rateLimiter.setRate(rateOut);

            } else { // trending down

              myRecord.dir="###DOWN###";

              if (retryCount[0]>0) {
                log("########## retryCount ##########", retryCount[0]);
                Thread.sleep(1000);
              }

// "rateIn":92.0,"instantRate":0.6,"fastRate":5.4,"slowRate":2.7,"dir":"FASTER","meter":"162 1/0.2/6.3 2.7/0.5/0.2","rateOut":-2967.2}

              // double multiplier = 0; // how aggressive

                    // var absDesire = Math.min(instantRate, fastRate);
              
              double rateIn = rateLimiter.getRate();

              double minRate = Math.min(fastRate, slowRate); //###TODO PONDER THIS
              // var minRate = Math.min(rateIn, Math.min(fastRate, slowRate)); //###TODO PONDER THIS
              // var desiredRate = Math.min(instantRate, slowRate);
              // var deltaRate = rateIn - minRate;

                    // var desiredRate = instantRate; // // want lowest non-zero
                    // if (fastRate > 0) {
                    //   if (fastRate < instantRate)
                    //     desiredRate = fastRate;
                    // }
              double ratio = 3/2 * Math.abs(fastRate-slowRate)/slowRate; // (to-from)/from

              // if (rateIn>minRate) {
              //   multiplier = (rateIn-minRate)/minRate;
              //   // if (mulitipli)
              // }

              double rateOut = rateIn - ratio*rateIn;

              if (rateOut>instantRate)
                rateOut=instantRate;
              if (rateOut < MINRATE)
                rateOut = MINRATE;

              myRecord.rateOut = rateOut;

              rateLimiter.setRate(rateOut);

            }

                        //THROTTLE
                        rateLimiter.acquire(consumedCapacityUnits.intValue());
                        //THROTTLE
                        
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

  static List<MetricRecord<?>> extractAllMetrics(MetricCollection metrics) {
    List<MetricRecord<?>> result = new ArrayList<>();
    extractAllMetrics(metrics, result);
    return result;
  }

  static void extractAllMetrics(MetricCollection metrics, List<MetricRecord<?>> extractedMetrics) {
    for (MetricRecord<?> metric : metrics) {
        extractedMetrics.add(metric);
    }
    metrics.children().forEach(child -> extractAllMetrics(child, extractedMetrics));
  }

  // convenience
  static <T> ListenableFuture<T> lf(CompletableFuture<T> cf) {
    return CompletableFuturesExtra.toListenableFuture(cf);
  }

  static void log(Object... args) {    
    System.out.println(Instant.now().toString()+" "+Joiner.on(" ").useForNull("null").join(args));
  }

}



// 2021-03-12T13:02:19.139856Z {"success":true,"rateIn":92.0,"instantRate":87.0,"fastRate":74.2,"slowRate":35.3,"dir":"FASTER","meter":"2117 28/53.6/77 35.3/7.1/2.4","rateOut":87.0}
// 2021-03-12T13:02:21.565885Z ########## retryCount ########## 4
// 2021-03-12T13:02:21.566497Z {"success":true,"rateIn":87.0,"instantRate":1.4,"fastRate":56.2,"slowRate":35.3,"dir":"FASTER","meter":"2118 1/17.4/62.7 35.3/7.1/2.4","rateOut":17.6}
// 2021-03-12T13:02:21.653237Z {"success":true,"rateIn":17.6,"instantRate":93.0,"fastRate":55.8,"slowRate":35.3,"dir":"FASTER","meter":"2119 2/17.2/62.3 35.3/7.1/2.4","rateOut":61.1}
// 2021-03-12T13:02:21.695008Z ########## retryCount ########## 3
// 2021-03-12T13:02:21.695394Z {"success":true,"rateIn":61.1,"instantRate":3.1,"fastRate":55.4,"slowRate":35.3,"dir":"FASTER","meter":"2120 3/16.6/62.2 35.3/7.1/2.4","rateOut":17.7}
// 2021-03-12T13:02:21.742766Z {"success":true,"rateIn":17.7,"instantRate":89.9,"fastRate":55.2,"slowRate":35.3,"dir":"FASTER","meter":"2121 3/16.6/61.8 35.3/7.1/2.4","rateOut":58.2}
// 2021-03-12T13:02:21.782110Z {"success":true,"rateIn":58.2,"instantRate":93.0,"fastRate":54.9,"slowRate":35.4,"dir":"FASTER","meter":"2122 4/16/61.7 35.4/7.1/2.4","rateOut":77.5}
// 2021-03-12T13:02:21.833203Z {"success":true,"rateIn":77.5,"instantRate":88.9,"fastRate":54.8,"slowRate":35.4,"dir":"FASTER","meter":"2123 6/16.4/61.5 35.4/7.1/2.4","rateOut":83.7}
// 2021-03-12T13:02:21.833522Z ########## retryCount ########## 3
// 2021-03-12T13:02:21.833826Z {"success":true,"rateIn":83.7,"instantRate":2.7,"fastRate":54.8,"slowRate":35.4,"dir":"FASTER","meter":"2124 6/16.4/61.5 35.4/7.1/2.4","rateOut":17.7}
