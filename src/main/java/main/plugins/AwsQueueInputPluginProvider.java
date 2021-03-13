package main.plugins;

import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

import main.InputPlugin;
import main.InputPluginProvider;
import main.helpers.AwsQueueMessageReceiver;

import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

class AwsQueueInputPlugin implements InputPlugin {

    private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

    private final AwsQueueMessageReceiver queueReceiver;

    public AwsQueueInputPlugin(AwsQueueMessageReceiver queueReceiver) {
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

}

@Service
public class AwsQueueInputPluginProvider implements InputPluginProvider {

    @Override
    public InputPlugin get(String arg, ApplicationArguments args) throws Exception {
        if (arg.startsWith("sqs:"))
            arg = arg.substring(arg.indexOf(":") + 1);
        if (arg.matches("https://sqs.(.+).amazonaws.(.*)/(\\d{12})/(.+)")) {
            String queueUrl = arg;
            return new AwsQueueInputPlugin(new AwsQueueMessageReceiver(queueUrl, Runtime.getRuntime().availableProcessors()));
        }
        return null;
    }

}
