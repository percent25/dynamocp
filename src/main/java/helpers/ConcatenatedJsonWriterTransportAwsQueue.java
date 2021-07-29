package helpers;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.futures.CompletableFuturesExtra;

import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * ConcatenatedJsonWriterTransportAwsQueue
 */
public class ConcatenatedJsonWriterTransportAwsQueue implements ConcatenatedJsonWriter.Transport {
    private final SqsAsyncClient client;
    private final String queueUrl;

    /**
     * ctor
     * 
     * @param client
     * @param queueUrl
     */
    public ConcatenatedJsonWriterTransportAwsQueue(SqsAsyncClient client, String queueUrl) {
        debug("ctor", queueUrl);
        this.client = client;
        this.queueUrl = queueUrl;
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("queueUrl", queueUrl).toString();
    }

    @Override
    public int mtuBytes() {
        return 256 * 1024; // sns/sqs max msg len
    }

    @Override
    public ListenableFuture<?> send(byte[] bytes) {
        trace("send", bytes.length);
        String messageBody = new String(bytes);
        SendMessageRequest request = SendMessageRequest.builder() //
                .queueUrl(queueUrl) //
                .messageBody(messageBody) //
                .build();
        return CompletableFuturesExtra.toListenableFuture(client.sendMessage(request));
    }

    private void debug(Object... args) {
        new LogHelper(this).debug(args);
    }

    private void trace(Object... args) {
        new LogHelper(this).trace(args);
    }

}
