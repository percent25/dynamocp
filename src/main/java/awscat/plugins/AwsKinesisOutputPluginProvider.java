package awscat.plugins;

import java.util.function.Supplier;

import com.google.gson.Gson;

import org.springframework.stereotype.Service;

import awscat.Args;
import awscat.OutputPlugin;
import awscat.OutputPluginProvider;
import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsKinesis;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

@Service
public class AwsKinesisOutputPluginProvider implements OutputPluginProvider {

    class Options extends AwsOptions {
        public String toString() {
            return new Gson().toJson(this);
        }
    }

    // private String streamArn;
    private Options options;

    @Override
    public String help() {
        return "kinesis:<stream-name>";
    }

    // @Override
    // public String toString() {
    //     return MoreObjects.toStringHelper(this).add("streamArn", streamArn).toString();
    // }

    @Override
    public boolean canActivate(String arg) {
        // streamArn = Args.base(arg);
        options = Args.options(arg, Options.class);

        return Args.base(arg).startsWith("kinesis:");

        // "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/MyStream",
        // return streamArn.matches("arn:(.+):kinesis:(.+):(\\d{12}):stream/(.+)");
    }

    @Override
    public Supplier<OutputPlugin> activate(String arg) throws Exception {
        KinesisAsyncClient client = AwsHelper.configClient(KinesisAsyncClient.builder(), options).build();

        String streamName = Args.base(arg).split(":")[1];

        // sqs transport is thread-safe
        ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsKinesis(client, streamName);
        
        // ConcatenatedJsonWriter is not thread-safe
        // which makes ConcatenatedJsonWriterOutputPlugin not thread-safe
        return ()->new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
    }
    
}