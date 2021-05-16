package awscat.plugins;

import java.util.function.Function;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import awscat.*;

import org.springframework.stereotype.Service;

import helpers.*;
import software.amazon.awssdk.services.sqs.*;

class AwsQueueInputPlugin implements InputPlugin {

    private final AwsQueueMessageReceiver messageReceiver;
    private int count;
    private final int limit;

    private Function<Iterable<JsonElement>, ListenableFuture<?>> listener;

    private final VoidFuture lf = new VoidFuture();

    public AwsQueueInputPlugin(AwsQueueMessageReceiver messageReceiver, int limit) {
        debug("ctor");
        this.messageReceiver = messageReceiver;
        this.limit = limit;
        messageReceiver.setListener(message->{
            return new FutureRunner(){{
                run(()->{
                    //###TODO THIS IS WRONG
                    //###TODO THIS IS WRONG
                    //###TODO THIS IS WRONG
                    ++count;
                    //###TODO THIS IS WRONG
                    //###TODO THIS IS WRONG
                    //###TODO THIS IS WRONG
                    return listener.apply(Lists.newArrayList(new JsonStreamParser(message)));
                }, ()->{
                    if (count > limit) {
                        messageReceiver.close();
                        lf.setVoid();
                    }
                });
            }}.get();
        });
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("messageReceiver", messageReceiver).toString();
    }

    @Override
    public ListenableFuture<?> read(int mtu) throws Exception {
        messageReceiver.start();
        return lf;
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

    class Options extends BaseOptions {
        public int c;
        public int limit;

        public String toString() {
            return new Gson().toJson(this);
        }
    }

    private String queueArnOrUrl;
    private Options options;

    @Override
    public String help() {
        return "<queue-url>[,c]";
    }

    public String toString() {
        // return new Gson().toJson(this);
        return MoreObjects.toStringHelper(this).add("queueArnOrUrl", queueArnOrUrl).add("options", options).toString();
      }

    @Override
    public boolean canActivate(String arg) {
        queueArnOrUrl = Args.base(arg);
        options = Args.options(arg, Options.class);
        if (queueArnOrUrl.matches("arn:(.+):sqs:(.+):(\\d{12}):(.+)"))
            return true;
        if (queueArnOrUrl.matches("https://queue.amazonaws.(.*)/(\\d{12})/(.+)"))
            return true;
        if (queueArnOrUrl.matches("https://sqs.(.+).amazonaws.(.*)/(\\d{12})/(.+)"))
            return true;
        return false;
    }

    @Override
    public InputPlugin activate(String arg) throws Exception {
        int c = options.c > 0 ? options.c : Runtime.getRuntime().availableProcessors();
        SqsAsyncClient sqsClient = AwsHelper.options(SqsAsyncClient.builder(), options).build();
        return new AwsQueueInputPlugin(new AwsQueueMessageReceiver(sqsClient, queueArnOrUrl, c), options.limit);
    }

}
