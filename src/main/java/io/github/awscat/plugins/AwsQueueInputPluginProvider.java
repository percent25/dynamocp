package io.github.awscat.plugins;

import java.util.function.Function;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import org.springframework.stereotype.Service;

import helpers.*;
import io.github.awscat.*;

class AwsQueueInputPlugin implements InputPlugin {

    private final AwsQueueMessageReceiver messageReceiver;

    private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

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

    @Override
    public String help() {
        return "<queue-url>[,c]";
    }

    @Override
    public boolean canActivate(String arg) {
        String queueUrl = Args.base(arg);
        if (queueUrl.matches("https://queue.amazonaws.(.*)/(\\d{12})/(.+)"))
            return true;
        if (queueUrl.matches("https://sqs.(.+).amazonaws.(.*)/(\\d{12})/(.+)"))
            return true;
        return false;
    }

    @Override
    public InputPlugin activate(String arg) throws Exception {
        String queueUrl = Args.base(arg);
        Options options = Args.options(arg, Options.class);
        int c = options.c > 0 ? options.c : Runtime.getRuntime().availableProcessors();
        return new AwsQueueInputPlugin(new AwsQueueMessageReceiver(queueUrl, c));
    }

}
