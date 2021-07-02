package awscat.plugins;

import java.util.function.Supplier;

import com.google.common.base.MoreObjects;
import com.google.gson.Gson;

import awscat.Addresses;
import awscat.OutputPlugin;
import awscat.OutputPluginProvider;

import org.springframework.stereotype.Service;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsTopic;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

@Service
public class AwsTopicOutputPluginProvider implements OutputPluginProvider{

    class Options extends AwsOptions {
        public String toString() {
            return new Gson().toJson(this);
        }
    }

    private String topicArn;
    private Options options;

    public String toString() {
        return MoreObjects.toStringHelper(this).add("topicArn", topicArn).toString();
    }

    @Override
    public String help() {
        return "<topic-arn>";
    }

    @Override
    public boolean canActivate(String address) {
        topicArn = Addresses.base(address);
        options = Addresses.options(address, Options.class);
        return topicArn.matches("arn:(.+):sns:(.+):(\\d{12}):(.+)");
    }

    @Override
    public Supplier<OutputPlugin> activate(String address) throws Exception {
        SnsAsyncClient snsClient = AwsHelper.build(SnsAsyncClient.builder(), options);
        // sns transport is thread-safe
        ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsTopic(snsClient, topicArn);
        // ConcatenatedJsonWriter is not thread-safe
        // which makes ConcatenatedJsonWriterOutputPlugin not thread-safe
        return ()->new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
    }

}