package helpers;

import com.google.common.base.MoreObjects;
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
        debug("ctor", client, topicArn);
        this.client = client;
        this.topicArn = topicArn;
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("client", client).add("topicArn", topicArn).toString();
    }

    @Override
    public int mtu() {
        return 256 * 1024; // sns/sqs max msg len
    }

    @Override
    public ListenableFuture<?> send(String message) {
        debug("send", message.length());
        PublishRequest publishRequest = PublishRequest.builder().topicArn(topicArn).message(message).build();
        return CompletableFuturesExtra.toListenableFuture(client.publish(publishRequest));
    }

    private void debug(Object... args) {
        new LogHelper(this).debug(args);
    }

}
