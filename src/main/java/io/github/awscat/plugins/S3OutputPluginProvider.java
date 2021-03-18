package io.github.awscat.plugins;

import java.io.File;
import java.util.function.Supplier;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsS3;
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
        return "s3".equals(args.getNonOptionArgs().get(1).split(":")[0]);
    }

    @Override
    public Supplier<OutputPlugin> get() throws Exception {
        String arg = args.getNonOptionArgs().get(1);
        // if (arg.startsWith("s3://"))
        {
            S3AsyncClient client = S3AsyncClient.create();
            //###TODO the whole s3://mybucket/myprefix should be used here
            //###TODO the whole s3://mybucket/myprefix should be used here
            //###TODO the whole s3://mybucket/myprefix should be used here
            String bucket = new File(Args.base(arg)).getName(); //###TODO the whole s3://mybucket/myprefix should be used here
            //###TODO the whole s3://mybucket/myprefix should be used here
            //###TODO the whole s3://mybucket/myprefix should be used here
            //###TODO the whole s3://mybucket/myprefix should be used here
            // Note- aws s3 transport is thread safe
            ConcatenatedJsonWriterTransportAwsS3 transport = new ConcatenatedJsonWriterTransportAwsS3(client, bucket, "awscat");
            return () -> {
                // Note- ConcatenatedJsonWriter is not thread safe
                // therefore need to return a new instance every invocation
                return new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
            };
        }
        // return null;
    }
    
}
