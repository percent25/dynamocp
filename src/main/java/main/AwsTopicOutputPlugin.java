package main;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import main.helpers.ConcatenatedJsonWriter;
import main.helpers.ConcatenatedJsonWriterTransportAwsTopic;
import main.helpers.LogHelper;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

public class AwsTopicOutputPlugin implements OutputPlugin {

    private final ConcatenatedJsonWriter writer;

    public AwsTopicOutputPlugin(ConcatenatedJsonWriter writer) {
        this.writer = writer;
    }
    @Override
    public ListenableFuture<?> write(Iterable<JsonElement> jsonElements) {
        for (JsonElement jsonElement : jsonElements) {
            var lf = writer.write(jsonElement);
            lf.addListener(()->{
                try {
                    lf.get();
                } catch (Exception e) {
                    log(e);
                }
            }, MoreExecutors.directExecutor());
        }
        return writer.flush();
    }
    
    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

}

@Service
class AwsTopicOutputPluginProvider implements OutputPluginProvider{

    @Override
    public OutputPlugin get(String arg, ApplicationArguments args) throws Exception {
        if (arg.startsWith("sns:")) {
            SnsAsyncClient client = SnsAsyncClient.create();
            String topicArn = arg.substring(arg.indexOf(":")+1);
            ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsTopic(client, topicArn);
            return new AwsTopicOutputPlugin(new ConcatenatedJsonWriter(transport));

        }
        return null;
    }

}