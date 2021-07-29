package helpers;

import java.util.UUID;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.futures.CompletableFuturesExtra;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

/**
 * ConcatenatedJsonWriterTransportAwsKinesis
 */
public class ConcatenatedJsonWriterTransportAwsKinesis implements ConcatenatedJsonWriter.Transport {
    private final KinesisAsyncClient client;
    private final String streamName;

    /**
     * ctor
     * 
     * @param client
     * @param streamName
     */
    public ConcatenatedJsonWriterTransportAwsKinesis(KinesisAsyncClient client, String streamName) {
        debug("ctor", streamName);
        this.client = client;
        this.streamName = streamName;
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("streamName", streamName).toString();
    }

    @Override
    public int mtuBytes() {
        return 1024 * 1024; // https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
    }

    @Override
    public ListenableFuture<?> send(byte[] bytes) {
        trace("send", bytes.length);
        String partitionKey = UUID.randomUUID().toString();
        SdkBytes data = SdkBytes.fromByteArray(bytes);
        PutRecordRequest request = PutRecordRequest.builder() //
                .streamName(streamName) //
                .partitionKey(partitionKey) //
                .data(data) //
                .build();
        return CompletableFuturesExtra.toListenableFuture(client.putRecord(request));
    }

    private void debug(Object... args) {
        new LogHelper(this).debug(args);
    }

    private void trace(Object... args) {
        new LogHelper(this).trace(args);
    }

}
