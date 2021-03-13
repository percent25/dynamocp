package main.plugins;

import java.io.File;
import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsS3;
import main.OutputPlugin;
import main.OutputPluginProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@Service
public class S3OutputPluginProvider implements OutputPluginProvider {

    @Override
    public Supplier<OutputPlugin> get(String arg, ApplicationArguments args) throws Exception {
        if (arg.startsWith("s3://")) {
            S3AsyncClient client = S3AsyncClient.create();
            String bucket = new File(arg).getName();
            // Note- aws s3 transport is threadsafe
            ConcatenatedJsonWriterTransportAwsS3 transport = new ConcatenatedJsonWriterTransportAwsS3(client, bucket, "awscat");
            return () -> {
                // Note- ConcatenatedJsonWriter is not thread safe
                // therefore need to return a new instance every invocation
                return new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
            };
        }
        return null;
    }
    
}
