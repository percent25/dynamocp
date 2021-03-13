package main.plugins;

import java.io.File;
import java.util.function.Supplier;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsS3;
import helpers.LogHelper;
import main.OutputPlugin;
import main.OutputPluginProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

        // class S3OutputPlugin implements OutputPlugin {

        //     private final ConcatenatedJsonWriter writer;

        //     public S3OutputPlugin(ConcatenatedJsonWriter writer) {
        //         this.writer = writer;
        //     }

        //     @Override
        //     public ListenableFuture<?> write(Iterable<JsonElement> jsonElements) {
        //         log("write", Iterables.size(jsonElements));
        //         for (JsonElement jsonElement : jsonElements) {
        //             var lf = writer.write(jsonElement);
        //             lf.addListener(()->{
        //                 try {
        //                     lf.get();
        //                 } catch (Exception e) {
        //                     log(e);
        //                 }
        //             }, MoreExecutors.directExecutor());
        //         }
        //         return writer.flush();
        //     }

        //     private void log(Object... args) {
        //         new LogHelper(this).log(args);
        //     }

        // }

@Service
public class S3OutputPluginProvider implements OutputPluginProvider {

    @Override
    public Supplier<OutputPlugin> get(String arg, ApplicationArguments args) throws Exception {
        if (arg.startsWith("s3://")) {
            S3AsyncClient client = S3AsyncClient.create();
            String bucket = new File(arg).getName();
            ConcatenatedJsonWriterTransportAwsS3 transport = new ConcatenatedJsonWriterTransportAwsS3(client, bucket, "awscat");
            ConcatenatedJsonWriter writer = new ConcatenatedJsonWriter(transport);
            return ()->new ConcatenatedJsonWriterOutputPlugin(writer);
        }
        return null;
    }
    
}
