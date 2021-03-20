package io.github.awscat.plugins;

import com.google.common.base.MoreObjects;
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

    public String toString() {
        return MoreObjects.toStringHelper(this).add("writer", writer).toString();
    }

    @Override
    public ListenableFuture<?> write(JsonElement jsonElement) {
        trace("write", jsonElement);
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

    private void trace(Object... args) {
        new LogHelper(this).trace(args);
    }

}
