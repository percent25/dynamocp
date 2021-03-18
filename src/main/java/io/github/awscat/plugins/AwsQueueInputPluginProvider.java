package io.github.awscat.plugins;

import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import helpers.AwsQueueMessageReceiver;
import helpers.LogHelper;
import io.github.awscat.InputPlugin;
import io.github.awscat.InputPluginProvider;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

class AwsQueueInputPlugin implements InputPlugin {

    private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

    private final AwsQueueMessageReceiver queueReceiver;

    public AwsQueueInputPlugin(AwsQueueMessageReceiver queueReceiver) {
        debug("ctor");
        this.queueReceiver = queueReceiver;
        queueReceiver.setListener(json->{
            return listener.apply(Lists.newArrayList(new JsonStreamParser(json)));
        });
    }

    @Override
    public ListenableFuture<?> read() throws Exception {
        queueReceiver.start();
        Thread.sleep(Long.MAX_VALUE);
        return Futures.immediateVoidFuture();
    }

    @Override
    public void setListener(Function<Iterable<JsonElement>, ListenableFuture<?>> listener) {
        this.listener = listener;
    }

    private void debug(Object... args) {
        new LogHelper(this).debug(args);
    }

}

@Service
public class AwsQueueInputPluginProvider implements InputPluginProvider {

    private final ApplicationArguments args;

    public AwsQueueInputPluginProvider(ApplicationArguments args) {
        this.args = args;
    }

    public boolean canActivate() {
        return args.getNonOptionArgs().get(0).matches("https://sqs.(.+).amazonaws.(.*)/(\\d{12})/(.+)");
    }

    @Override
    public InputPlugin get() throws Exception {
        String arg = args.getNonOptionArgs().get(0);
        if (arg.matches("https://sqs.(.+).amazonaws.(.*)/(\\d{12})/(.+)")) {
            String queueUrl = arg;
            int concurrency = Runtime.getRuntime().availableProcessors();
            return new AwsQueueInputPlugin(new AwsQueueMessageReceiver(queueUrl, concurrency));
        }
        return null;
    }

}
