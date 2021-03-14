package io.github.awscat.plugins;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;

import helpers.ConcatenatedJsonWriter;
import helpers.LogHelper;
import io.github.awscat.OutputPlugin;

class ConcatenatedJsonWriterOutputPlugin implements OutputPlugin {

    private final ConcatenatedJsonWriter writer;

    public ConcatenatedJsonWriterOutputPlugin(ConcatenatedJsonWriter writer) {
        this.writer = writer;
    }

    @Override
    public ListenableFuture<?> write(Iterable<JsonElement> jsonElements) {
        log("write", Iterables.size(jsonElements));
        for (JsonElement jsonElement : jsonElements) {
            var lf = writer.write(jsonElement);
            lf.addListener(()->{
                try {
                    lf.get();
                } catch (Exception e) {
                    log(e);
                }
            }, MoreExecutors.directExecutor());
        }
        return writer.flush();
    }

    private void log(Object... args) {
        new LogHelper(this).log(args);
    }

}
