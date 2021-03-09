package app;

import io.netty.util.NettyRuntime;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

public class AwsSdkTwo {

  public static SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
      //
      // .maxConcurrency(50 * NettyRuntime.availableProcessors())
      //
      // .maxPendingConnectionAcquires(10_000)
      //
      .build();

}
