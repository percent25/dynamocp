package awscat.plugins;

import java.net.URI;
import java.util.function.Supplier;

import com.google.gson.Gson;

import org.springframework.stereotype.Service;

import awscat.Addresses;
import awscat.OutputPlugin;
import awscat.OutputPluginProvider;
import helpers.ConcatenatedJsonWriter;
import helpers.ConcatenatedJsonWriterTransportAwsS3Export;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@Service
// @Help("s3://<bucket>/<prefix>")
public class S3OutputPluginProvider implements OutputPluginProvider {

  class Options extends AwsOptions {
    public String toString() {
      return new Gson().toJson(this);
    }
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

    URI uri = URI.create(Addresses.base(arg));
    String bucket = uri.getHost();
    String exportPrefix = uri.getPath();
    // s3://mybucket -> ""
    // s3://mybucket/ -> ""
    // s3://mybucket/a -> "/a"
    if (exportPrefix.startsWith("/"))
      exportPrefix = exportPrefix.substring(1);

    Options options = Addresses.options(arg, Options.class);

    S3AsyncClient client = AwsHelper.create(S3AsyncClient.builder(), options);

    // Note- transport is thread safe
    ConcatenatedJsonWriter.Transport transport = new ConcatenatedJsonWriterTransportAwsS3Export(client, bucket,
        exportPrefix);
    // Note- ConcatenatedJsonWriter is not thread safe
    return () -> {
      return new ConcatenatedJsonWriterOutputPlugin(new ConcatenatedJsonWriter(transport));
    };
  }

}
