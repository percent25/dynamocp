package io.github.awscat.plugins;

import java.net.URI;
import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsS3Export;
import io.github.awscat.Args;
import io.github.awscat.OutputPlugin;
import io.github.awscat.OutputPluginProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@Service
public class S3OutputPluginProvider implements OutputPluginProvider {

    private final ApplicationArguments args;

    public S3OutputPluginProvider(ApplicationArguments args) {
        this.args = args;
    }

    @Override
    public boolean canActivate() {
        String arg = args.getNonOptionArgs().get(1);
        return "s3".equals(Args.base(arg).split(":")[0]);
    }

    @Override
    public Supplier<OutputPlugin> get() throws Exception {
        String arg = args.getNonOptionArgs().get(1);
        S3AsyncClient client = S3AsyncClient.create();

        URI uri = URI.create(Args.base(arg));
        String bucket = uri.getHost();
        String exportPrefix = uri.getPath().substring(1);

        // Note- aws s3 transport is thread safe
        var transport = new ConcatenatedJsonWriterTransportAwsS3Export(client, bucket, exportPrefix);
        return () -> {
            // Note- ConcatenatedJsonWriter is not thread safe
            // therefore need to return a new instance every invocation
            return new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
        };
    }
    
}
