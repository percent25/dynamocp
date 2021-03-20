package io.github.awscat.plugins;

import java.util.function.Supplier;

import com.google.common.base.MoreObjects;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsTopic;
import io.github.awscat.Args;
import io.github.awscat.OutputPlugin;
import io.github.awscat.OutputPluginProvider;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

@Service
public class AwsTopicOutputPluginProvider implements OutputPluginProvider{

    private final ApplicationArguments args;
    private String topicArn;

    public AwsTopicOutputPluginProvider(ApplicationArguments args) {
        this.args = args;
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("topicArn", topicArn).toString();
    }

    @Override
    public boolean canActivate() {
        String arg = args.getNonOptionArgs().get(1);
        topicArn = Args.base(arg);
        return topicArn.matches("arn:(.+):sns:(.+):(\\d{12}):(.+)");
    }

    @Override
    public Supplier<OutputPlugin> get() throws Exception {
        SnsAsyncClient client = SnsAsyncClient.create();
        // sns transport is thread-safe
        ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsTopic(client, topicArn);
        // ConcatenatedJsonWriter is not thread-safe
        // which makes ConcatenatedJsonWriterOutputPlugin not thread-safe
        return ()->new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
    }

}