package helpers;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import com.google.common.base.Ascii;
import com.google.common.base.Defaults;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * ConcatenatedJsonWriter
 * 
 * <p>pipelined
 * <p>not thread-safe
 */
public class ConcatenatedJsonWriter {

    public interface Transport {
        /**
         * maximum transmission unit
         */
        int mtu();

        /**
         * send message
         * 
         * <p>
         * ConcatenatedJsonWriter shall not ask Transport to send a message more than mtu
         */
        ListenableFuture<?> send(String message);
    }

    private class VoidFuture extends AbstractFuture<Void> {
        public boolean setVoid() {
            return super.set(Defaults.defaultValue(Void.class));
        }

        public boolean setException(Throwable throwable) {
            return super.setException(throwable);
        }
    }

    private final Transport transport;

    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final Multimap<ByteArrayOutputStream, VoidFuture> partitions = Multimaps.synchronizedMultimap(LinkedListMultimap.create());
    private final List<ListenableFuture<?>> flushFutures = Lists.newCopyOnWriteArrayList();

    /**
     * ctor
     * 
     * @param transport
     * @param tags
     */
    public ConcatenatedJsonWriter(Transport transport) {
        debug("ctor");
        this.transport = transport;
    }

    /**
     * write
     * 
     * @param jsonElement
     * @return
     */
    public ListenableFuture<?> write(JsonElement jsonElement) {
        return new FutureRunner() {
            {
                run(() -> {
                    byte[] bytes = render(jsonElement);
                    if (bytes.length > transport.mtu())
                        throw new IllegalArgumentException("jsonElement more than mtu");
                    if (baos.size() + bytes.length > transport.mtu())
                        baos = flush(baos, partitions.get(baos));
                    baos.write(bytes, 0, bytes.length);

                    VoidFuture lf = new VoidFuture();
                    partitions.put(baos, lf); // track futures on a per-baos/partition basis
                    return lf;
                });
            }
        }.get();
    }

    class FlushWork {
        long in;
        long inErr;
        long out;
        long outErr;
        public String toString() {
            return new Gson().toJson(this);
        }
    }

    /**
     * flush
     * 
     * @return
     */
    public ListenableFuture<?> flush() {
        return new FutureRunner() {
            FlushWork work = new FlushWork();
            {
                run(() -> {
                    if (baos.size() > 0)
                        baos = flush(baos, partitions.get(baos));
                    return Futures.successfulAsList(flushFutures);
                });
            }
            @Override
            protected void onListen() {
                debug(work);
            }
        }.get();
    }

    // returns new baos
    private ByteArrayOutputStream flush(ByteArrayOutputStream baos, Iterable<VoidFuture> partition) {
        var lf = new FutureRunner() {
            {
                run(() -> {
                    // request
                    return transport.send(baos.toString());
                }, sendResponse -> {
                    // success
                    partition.forEach(lf -> lf.setVoid());
                }, e -> {
                    // failure
                    partition.forEach(lf -> lf.setException(e));
                });
            }
        }.get();
        flushFutures.add(lf);
        return new ByteArrayOutputStream();
    }

    private byte[] render(JsonElement jsonElement) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new PrintStream(baos, true).println(jsonElement.toString());
        return baos.toByteArray();
    }

    private void debug(Object... args) {
        new LogHelper(this).debug(args);
    }

}