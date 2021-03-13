package main.plugins;

import java.util.function.Supplier;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsTopic;
import helpers.LogHelper;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import main.OutputPlugin;
import main.OutputPluginProvider;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

// class AwsTopicOutputPlugin implements OutputPlugin {

//     private final ConcatenatedJsonWriter writer;

//     public AwsTopicOutputPlugin(ConcatenatedJsonWriter writer) {
//         log("ctor");
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
public class AwsTopicOutputPluginProvider implements OutputPluginProvider{

    @Override
    public Supplier<OutputPlugin> get(String arg, ApplicationArguments args) throws Exception {
        if (arg.startsWith("sns:"))
            arg = arg.substring(arg.indexOf(":")+1);
        if (arg.matches("arn:(.+):sns:(.+):(\\d{12}):(.+)")) {
            String topicArn = arg;
            SnsAsyncClient client = SnsAsyncClient.create();
            ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsTopic(client, topicArn);
            return ()->new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
        }
        return null;
    }

}