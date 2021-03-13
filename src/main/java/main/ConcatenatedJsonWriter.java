package main;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.google.common.base.Ascii;
import com.google.common.base.Defaults;
import com.google.common.collect.LinkedListMultimap;
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

    /**
     * ctor
     * 
     * @param transport
     * @param tags
     */
    public ConcatenatedJsonWriter(Transport transport) {
        log("ctor");
        this.transport = transport;
    }

    class WriteRecord {
        public boolean success;
        public String failureMessage;
        public String value;
        public String toString() {
            return new Gson().toJson(this);
        }
    }

    /**
     * write
     * 
     * @param jsonElement
     * @return
     */
    public ListenableFuture<?> write(JsonElement jsonElement) {
        return new FutureRunner() {
            WriteRecord record = new WriteRecord();
            {
                run(() -> {
                    // for fun
                    record.value = Ascii.truncate(jsonElement.toString(), 20, "...");

                    byte[] bytes = render(jsonElement);
                    if (bytes.length > transport.mtu())
                        throw new IllegalArgumentException("jsonElement more than mtu");
                    if (baos.size() + bytes.length > transport.mtu())
                        baos = flush(baos, partitions.get(baos));
                    baos.write(bytes, 0, bytes.length);

                    VoidFuture lf = new VoidFuture();
                    partitions.put(baos, lf); // track futures on a per-baos/partition basis
                    return lf;
                }, result->{
                    record.success = true;
                }, e->{
                    record.failureMessage = e.toString();
                    throw new RuntimeException(e); // propagate to caller
                }, ()->{
                    // log(record);
                });
            }
        }.get();
    }

    class FlushRecord {
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
            FlushRecord record = new FlushRecord();
            {
                run(() -> {
                    if (baos.size() > 0)
                        baos = flush(baos, partitions.get(baos));
                    return Futures.successfulAsList(partitions.values());
                }, () -> {
                    // log(record);
                });
            }
        }.get();
    }

    // returns new baos
    private ByteArrayOutputStream flush(ByteArrayOutputStream baos, Iterable<VoidFuture> partition) {
        //###TODO really a fire-forget? e.g., what if want to cancel??
        //###TODO really a fire-forget? e.g., what if want to cancel??
        //###TODO really a fire-forget? e.g., what if want to cancel??
        new FutureRunner() { // front facade not interesting.. inside futures interesting
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
        };
        return new ByteArrayOutputStream();
    }

    private byte[] render(JsonElement jsonElement) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new PrintStream(baos, true).println(jsonElement.toString());
        return baos.toByteArray();
    }

    private static void log(Object... args) {
        new LogHelper(ConcatenatedJsonWriter.class).log(args);
    }

}