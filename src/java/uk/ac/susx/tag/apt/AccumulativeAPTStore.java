package uk.ac.susx.tag.apt;

import clojure.lang.APersistentMap;
import clojure.lang.PersistentHashMap;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Write only, accumulative APTStore, highly optimised for dealing with *big* data.
 * @author ds300
 */
public class AccumulativeAPTStore implements AutoCloseable {


    // immutable map from Integer -> AccumulativeLazyAPT
    private APersistentMap cache = PersistentHashMap.EMPTY;

    // when JVM numberOfBytesUsed/numberOfBytesAvailable > memoryFullThreshold the cache gets invalidated, and merged
    // into existing APTs in the backend.
    double memoryFullThreshold = 0.8;

    private final PersistentKVStore<Integer, byte[]> backend;

    final APTFactory<AccumulativeLazyAPT> factory = new AccumulativeLazyAPT.Factory();

    // merge depth for incoming APTs
    final int depth;

    boolean closed = false;

    volatile boolean clearingCache = false;

    public boolean isClearingCache() {
        return clearingCache;
    }

    Thread memoryWatcher = new Thread() {
        Runtime rt = Runtime.getRuntime();
        double mem() {
                   return (double) (rt.totalMemory() - rt.freeMemory()) / rt.maxMemory();
                                                                                            }
        @Override
        public void run(){
            while (!closed) {
                if (mem() > memoryFullThreshold) {
                    try {
                        System.out.println("Memory limit reached at " + ((rt.totalMemory() - rt.freeMemory()) / (1024 * 1024)) + "mb");
                        clearCache();
                        System.gc();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    };

    {
        memoryWatcher.start();
    }

    public AccumulativeAPTStore(PersistentKVStore<Integer, byte[]> backend, int depth) {
        this.backend = backend;
        this.depth = depth;
    }

    private AtomicInteger numCacheItemsCleared = new AtomicInteger();
    public int numCacheItemsCleared () {
        return numCacheItemsCleared.get();
    }

    private class PutTask implements Runnable {
        final int k;
        final AccumulativeLazyAPT apt;

        private PutTask(int k, AccumulativeLazyAPT apt) {
            this.k = k;
            this.apt = apt;
        }

        @Override
        public void run() {
            try {
                byte[] existing = backend.get(k);
                if (existing != null) {
                    backend.put(k, apt.mergeIntoExisting(existing));
                } else {
                    backend.put(k, apt.toByteArray());
                }
                numCacheItemsCleared.incrementAndGet();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private synchronized void clearCache() throws IOException {
        System.out.println("clearing cache...");
        clearingCache = true;
        numCacheItemsCleared.set(0);
        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 2);
        APersistentMap m = cache;
        cache = PersistentHashMap.EMPTY;

        for (Object o : m.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            int k = (int) e.getKey();
            AccumulativeLazyAPT apt = (AccumulativeLazyAPT) e.getValue();
            pool.submit(new PutTask(k, apt));
        }
        m = null;
        System.gc();
        pool.shutdown();
        try {
            pool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        clearingCache = false;
        System.out.println("... finished clearing cache");
    }

    private AccumulativeLazyAPT getCached(int key) throws IOException {
        Object apt = cache.valAt(key);
        if (apt == null) {
            AccumulativeLazyAPT e = factory.empty();
            return putCached(key, e);
        } else {
            return (AccumulativeLazyAPT) apt;
        }
    }

    // synchronized puts into cache rather than CAS. Seems to be faster.
    private synchronized AccumulativeLazyAPT putCached(int key, AccumulativeLazyAPT apt) throws IOException {
        Object existing = cache.valAt(key);
        if (existing == null)
            cache = (APersistentMap) cache.assoc(key, apt);
        else
            apt = (AccumulativeLazyAPT) existing;

        return apt;
    }

    public void include(int entityID, APT apt) throws IOException {
        assertNotClosed();
        for (;;) {
            try {
                getCached(entityID).merge(apt, depth, 0);
                return;
            } catch (AccumulativeLazyAPT.FrozenException ignored) {}
        }
    }

    public AccumulativeAPTStore setMemoryFullThreshold(double d) {
        if (d <=0 || d >= 1) throw new IllegalArgumentException("d must be between 0 and 1 exclusive");

        memoryFullThreshold = d;
        return this;
    }

    private void assertNotClosed() {
        if (closed) throw new IllegalStateException("Attempting to use lexicon after close");
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        System.out.println("closing accumulative apt store");
        closed = true;
        clearCache();
        backend.close();
        try {
            memoryWatcher.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
