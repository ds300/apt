package uk.ac.susx.tag.apt;

import clojure.lang.APersistentMap;
import clojure.lang.PersistentHashMap;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author ds300
 */
public class AccumulativeAPTStore implements APTStore<AccumulativeLazyAPT> {

    // immutable map from Integer -> AccumulativeLazyAPT
    private APersistentMap cache = PersistentHashMap.EMPTY;

    // when JVM numberOfBytesUsed/numberOfBytesAvailable > memoryFullThreshold the cache gets invalidated, and merged
    // into existing APTs in the backend.
    double memoryFullThreshold = 0.8;

    private final PersistentKVStore<Integer, byte[]> backend;

    final APTFactory<AccumulativeLazyAPT> factory = new AccumulativeLazyAPT.Factory();

    final ArrayAPT.Factory aaptFactory = new ArrayAPT.Factory();

    // merge depth for incoming APTs
    final int depth;

    boolean closed = false;

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
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private synchronized void clearCache() throws IOException {
        System.out.println("clearing cache...");
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
        pool.shutdown();
        try {
            pool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
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





    @Override
    public void include(int entityID, APT apt) throws IOException {
        assertNotClosed();
        for (;;) {
            try {
                getCached(entityID).merge(apt, depth, 0);
                return;
            } catch (FrozenException ignored) {
                System.out.println("frozed");
            }
        }
    }



    public void include(RGraph graph) throws IOException {
        assertNotClosed();
        ArrayAPT[] apts = aaptFactory.fromGraph(graph);
        for (int i=0; i<apts.length; i++) {
            ArrayAPT apt = apts[i];
            int eid = graph.entityIds[i];
            if (apt != null && eid != -1) {
                include(eid, apt);
            }
        }
    }

    public AccumulativeAPTStore setMemoryFullThreshold(double d) {
        if (d <=0 || d >= 1) throw new IllegalArgumentException("d must be between 0 and 1 exclusive");

        memoryFullThreshold = d;
        return this;
    }


    @Override
    @Deprecated
    public AccumulativeLazyAPT get(Integer entityID) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void put(Integer entityId, AccumulativeLazyAPT apt) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void remove(Integer entityID) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public boolean containsKey(Integer entityID) throws IOException {
        throw new UnsupportedOperationException();
    }



    private void assertNotClosed() {
        if (closed) throw new IllegalStateException("Attempting to use lexicon after close");
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        clearCache();
        backend.close();
        try {
            memoryWatcher.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static class Builder implements APTStoreBuilder<AccumulativeLazyAPT> {

        PersistentKVStore<Integer, byte[]> backend;
        int depth;

        @Override
        public APTStoreBuilder setBackend(PersistentKVStore<Integer, byte[]> backend) {
            this.backend = backend;
            return this;
        }

        @Override
        public APTStoreBuilder setFactory(APTFactory<AccumulativeLazyAPT> factory) {
            throw new UnsupportedOperationException("AccumulativeAPTStore supplies it's own factory");
        }

        public APTStoreBuilder setMaxDepth(int depth) {
            this.depth = depth;
            return this;
        }

        @Override
        public APTStore<AccumulativeLazyAPT> build() {
            if (backend == null) throw new UnsupportedOperationException("AccumulativeAPTStore requires a backend");
            else return new AccumulativeAPTStore(backend, depth);
        }
    }
}
