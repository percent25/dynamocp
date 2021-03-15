package io.github.awscat.plugins;

import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsTopic;
import io.github.awscat.OutputPlugin;
import io.github.awscat.OutputPluginProvider;
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
            // sns transport is thread-safe
            ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsTopic(client, topicArn);
            // ConcatenatedJsonWriter is not thread-safe
            // which makes ConcatenatedJsonWriterOutputPlugin not thread-safe
            return ()->new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
        }
        return null;
    }

}