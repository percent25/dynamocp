package helpers;

import com.google.common.base.*;
import com.google.common.util.concurrent.*;
import com.spotify.futures.*;

import software.amazon.awssdk.services.sns.*;
import software.amazon.awssdk.services.sns.model.*;

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
        debug("ctor", topicArn);
        this.client = client;
        this.topicArn = topicArn;
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("topicArn", topicArn).toString();
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
