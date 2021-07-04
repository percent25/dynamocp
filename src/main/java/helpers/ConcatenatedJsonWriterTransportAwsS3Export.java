package helpers;

import java.security.SecureRandom;
import java.time.Instant;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.futures.CompletableFuturesExtra;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataExport.Output.html
public class ConcatenatedJsonWriterTransportAwsS3Export implements ConcatenatedJsonWriter.Transport {
    private final S3AsyncClient client;
    private final String bucket;
    private final String exportPrefix;
    // private final String exportId;

    /**
     * ctor
     * 
     * @param client
     */
    public ConcatenatedJsonWriterTransportAwsS3Export(S3AsyncClient client, String bucket, String exportPrefix) {
        debug("ctor", client, exportPrefix);
        this.client = client;
        this.bucket = bucket;
        this.exportPrefix = exportPrefix;
        // this.exportId = exportId();
    }

    @Override
    public int mtu() {
        return 128 * 1024 * 1024; // like kinesis firehose
    }

    @Override
    public ListenableFuture<?> send(byte[] bytes) {

        // export-prefix/AWSDynamoDB/ExportId/manifest-files.json
        // export-prefix/AWSDynamoDB/ExportId/data/bafybeiczss3yxay3o4abnabbb.json.gz
        // export-prefix/AWSDynamoDB/ExportId/data/gkes5o3lnrhoznhnkyax3hxvya.json.gz

        String key = String.format("%s", dataObject());
        if (has(exportPrefix))
            key = String.format("%s/%s", exportPrefix, dataObject());
        // String key = String.format("%s/awscat/%s/%s", exportPrefix, exportId, dataObject());
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucket).key(key).build();
        AsyncRequestBody requestBody = AsyncRequestBody.fromBytes(bytes);
        return CompletableFuturesExtra.toListenableFuture(client.putObject(putObjectRequest, requestBody));
    }

    // // e.g., 20210619-c250b63
    // static String exportId() {
    //     // from aws docs: export-prefix/AWSDynamoDB/ExportId/data/bafybeiczss3yxay3o4abnabbb.json.gz
    //     String ymd = CharMatcher.anyOf("1234567890").retainFrom(Instant.now().toString().substring(0, 10));
    //     String randomString = Hashing.sha256().hashInt(new SecureRandom().nextInt()).toString().substring(0, 7);
    //     return String.format("%s-%s", ymd, randomString);
    // }

    // e.g., 20210619113500-e3bcadf.json
    static String dataObject() {
        // from aws docs: export-prefix/AWSDynamoDB/ExportId/data/bafybeiczss3yxay3o4abnabbb.json.gz
        String ymdhms = CharMatcher.anyOf("1234567890").retainFrom(Instant.now().toString().substring(0, 20));
        String randomString = Hashing.sha256().hashInt(new SecureRandom().nextInt()).toString().substring(0, 7);
        return String.format("%s-%s.json", ymdhms, randomString);
    }

    private boolean has(String s) {
        return Strings.nullToEmpty(s).length()>0;
    }

    private void debug(Object... args) {
        new LogHelper(this).debug(args);
    }

    public static void main(String... args) {
        // System.out.println(exportId());
        System.out.println(dataObject());
    }

}
