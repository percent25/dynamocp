package awscat.plugins;

import java.util.function.Supplier;

import com.google.common.base.MoreObjects;

import awscat.Args;
import awscat.OutputPlugin;
import awscat.OutputPluginProvider;

import org.springframework.stereotype.Service;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsTopic;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

@Service
public class AwsTopicOutputPluginProvider implements OutputPluginProvider{

    private String topicArn;

    public String toString() {
        return MoreObjects.toStringHelper(this).add("topicArn", topicArn).toString();
    }

    @Override
    public String help() {
        return "<topic-arn>";
    }

    @Override
    public boolean canActivate(String arg) {
        topicArn = Args.base(arg);
        return topicArn.matches("arn:(.+):sns:(.+):(\\d{12}):(.+)");
    }

    @Override
    public Supplier<OutputPlugin> activate(String arg) throws Exception {
        SnsAsyncClient client = SnsAsyncClient.create();
        // sns transport is thread-safe
        ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsTopic(client, topicArn);
        // ConcatenatedJsonWriter is not thread-safe
        // which makes ConcatenatedJsonWriterOutputPlugin not thread-safe
        return ()->new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
    }

}