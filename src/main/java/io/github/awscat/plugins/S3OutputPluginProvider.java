package io.github.awscat.plugins;

import java.net.*;
import java.util.function.*;

import org.springframework.stereotype.*;

import helpers.*;
import io.github.awscat.*;
import software.amazon.awssdk.services.s3.*;

@Service
public class S3OutputPluginProvider implements OutputPluginProvider {

    @Override
    public String help() {
        return "s3://<bucket>/<prefix>";
    }

    @Override
    public boolean canActivate(String arg) {
        return "s3".equals(arg.split(":")[0]);
    }

    @Override
    public Supplier<OutputPlugin> activate(String arg) throws Exception {
        S3AsyncClient client = S3AsyncClient.create();

        URI uri = URI.create(Args.base(arg));
        String bucket = uri.getHost();
        String exportPrefix = uri.getPath().substring(1);

        // Note- transport is thread safe
        ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsS3Export(client, bucket, exportPrefix);
        // Note- ConcatenatedJsonWriter is not thread safe
        return ()->{
            return new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
        };
    }

}
