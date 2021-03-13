package main.plugins;

import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsTopic;
import main.OutputPlugin;
import main.OutputPluginProvider;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

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