package io.github.awscat.plugins;

import java.util.function.Function;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import helpers.AwsQueueMessageReceiver;
import helpers.LogHelper;
import io.github.awscat.Args;
import io.github.awscat.InputPlugin;
import io.github.awscat.InputPluginProvider;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

class AwsQueueInputPlugin implements InputPlugin {

    private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

    private final AwsQueueMessageReceiver messageReceiver;

    public AwsQueueInputPlugin(AwsQueueMessageReceiver messageReceiver) {
        debug("ctor");
        this.messageReceiver = messageReceiver;
        messageReceiver.setListener(json->{
            return listener.apply(Lists.newArrayList(new JsonStreamParser(json)));
        });
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("messageReceiver", messageReceiver).toString();
    }

    @Override
    public ListenableFuture<?> read(int mtu) throws Exception {
        messageReceiver.start();
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

    class Options {
        int c;
    }

    private final ApplicationArguments args;

    public AwsQueueInputPluginProvider(ApplicationArguments args) {
        this.args = args;
    }

    @Override
    public boolean canActivate() {
        String arg = args.getNonOptionArgs().get(0);
        String queueUrl = Args.base(arg);
        if (queueUrl.matches("https://queue.amazonaws.(.*)/(\\d{12})/(.+)"))
            return true;
        if (queueUrl.matches("https://sqs.(.+).amazonaws.(.*)/(\\d{12})/(.+)"))
            return true;
        return false;
    }

    @Override
    public InputPlugin activate() throws Exception {
        String arg = args.getNonOptionArgs().get(0);
        String queueUrl = Args.base(arg);
        Options options = Args.options(arg, Options.class);
        int c = options.c > 0 ? options.c : Runtime.getRuntime().availableProcessors();
        return new AwsQueueInputPlugin(new AwsQueueMessageReceiver(queueUrl, c));
    }

}
