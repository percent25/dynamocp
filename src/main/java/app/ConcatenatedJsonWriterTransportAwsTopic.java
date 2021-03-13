package app;

import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.futures.CompletableFuturesExtra;

import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

/**
 * ConcatenatedJsonWriterTransportAwsTopic
 */
public class ConcatenatedJsonWriterTransportAwsTopic implements ConcatenatedJsonWriter.Transport {
    private final SnsAsyncClient client;
    private final String topicArn;

    /**
     * ctor
     * 
     * @param client
     * @param topicArn
     */
    public ConcatenatedJsonWriterTransportAwsTopic(SnsAsyncClient client, String topicArn) {
        this.client = client;
        this.topicArn = topicArn;
    }

    @Override
    public int mtu() {
        return 256 * 1024; // sns/sqs max msg len
    }

    @Override
    public ListenableFuture<?> send(String message) {
        PublishRequest publishRequest = PublishRequest.builder().topicArn(topicArn).message(message).build();
        return CompletableFuturesExtra.toListenableFuture(client.publish(publishRequest));
    }

}
