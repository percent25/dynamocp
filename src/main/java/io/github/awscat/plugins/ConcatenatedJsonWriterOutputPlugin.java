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
        debug("ctor");
        this.writer = writer;
    }

    @Override
    public ListenableFuture<?> write(JsonElement jsonElement) {
        debug("write");
        return writer.write(jsonElement);
    }

    @Override
    public ListenableFuture<?> flush() {
        debug("flush");
        return writer.flush();
    }    

    private void debug(Object... args) {
        new LogHelper(this).debug(args);
    }

}
