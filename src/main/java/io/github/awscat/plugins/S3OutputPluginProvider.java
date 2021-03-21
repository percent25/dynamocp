package io.github.awscat.plugins;

import java.net.URI;
import java.util.function.Supplier;

import com.google.common.base.MoreObjects;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsS3Export;
import io.github.awscat.Args;
import io.github.awscat.OutputPlugin;
import io.github.awscat.OutputPluginProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

// delegates toString to the provider
class AbstractSupplier implements Supplier<OutputPlugin> {

    private final OutputPluginProvider provider;
    private final Supplier<OutputPlugin> supplier;

    AbstractSupplier(OutputPluginProvider provider, Supplier<OutputPlugin> supplier) {
        this.provider = provider;
        this.supplier = supplier;
    }

    @Override
    public OutputPlugin get() {
        return supplier.get();
    }

    public String toString() {
        return provider.toString();
    }

}

@Service
public class S3OutputPluginProvider implements OutputPluginProvider {

    private final ApplicationArguments args;

    public S3OutputPluginProvider(ApplicationArguments args) {
        this.args = args;
    }

    public String toString() {
        String arg = args.getNonOptionArgs().get(1);
        return MoreObjects.toStringHelper(this).add("arg", arg).toString();
    }

    @Override
    public boolean canActivate(String arg) {
        return "s3".equals(arg.split(":")[0]);
    }

    @Override
    public Supplier<OutputPlugin> get(String arg) throws Exception {
        S3AsyncClient client = S3AsyncClient.create();

        URI uri = URI.create(Args.base(arg));
        String bucket = uri.getHost();
        String exportPrefix = uri.getPath().substring(1);

        // Note- transport is thread safe
        var transport = new ConcatenatedJsonWriterTransportAwsS3Export(client, bucket, exportPrefix);
        // Note- ConcatenatedJsonWriter is not thread safe
        return new AbstractSupplier(this, ()->{
            return new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
        });
    }

}
