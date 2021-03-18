package io.github.awscat.plugins;

import java.util.function.Supplier;

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

    public AwsTopicOutputPluginProvider(ApplicationArguments args) {
        this.args = args;
    }

    @Override
    public boolean canActivate() {
        String arg = args.getNonOptionArgs().get(1);
        return Args.base(arg).matches("arn:(.+):sns:(.+):(\\d{12}):(.+)");
    }

    @Override
    public Supplier<OutputPlugin> get() throws Exception {
        String arg = args.getNonOptionArgs().get(1);
        // if (arg.startsWith("sns:"))
        //     arg = arg.substring(arg.indexOf(":")+1);
        // if (arg.matches("arn:(.+):sns:(.+):(\\d{12}):(.+)"))
        {
            String topicArn = Args.base(arg);
            SnsAsyncClient client = SnsAsyncClient.create();
            // sns transport is thread-safe
            ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsTopic(client, topicArn);
            // ConcatenatedJsonWriter is not thread-safe
            // which makes ConcatenatedJsonWriterOutputPlugin not thread-safe
            return ()->new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
        }
        // return null;
    }

}