package awscat.plugins;

import java.net.*;
import java.util.function.*;

import org.springframework.stereotype.*;

import awscat.*;
import helpers.*;
import software.amazon.awssdk.services.s3.*;

@Service
// @Help("s3://<bucket>/<prefix>")
public class S3OutputPluginProvider implements OutputPluginProvider {

    class Options extends AwsOptions {

    }

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

        URI uri = URI.create(Args.base(arg));
        String bucket = uri.getHost();
        String exportPrefix = uri.getPath().substring(1);

        Options options = Args.options(arg, Options.class);

        S3AsyncClient client = AwsHelper.configClient(S3AsyncClient.builder(), options).build();

        // Note- transport is thread safe
        ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsS3Export(client, bucket, exportPrefix);
        // Note- ConcatenatedJsonWriter is not thread safe
        return ()->{
            return new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
        };
    }

}
