package uk.ac.susx.tag.apt;

import clojure.lang.APersistentMap;
import clojure.lang.PersistentHashMap;

import java.io.IOException;
import java.util.Map;

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



    private synchronized void clearCache() throws IOException {
        System.out.println("clearing cache...");
        APersistentMap m = cache;
        cache = PersistentHashMap.EMPTY;

        for (Object o : m.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            int k = (int) e.getKey();
            AccumulativeLazyAPT apt = (AccumulativeLazyAPT) e.getValue();
            byte[] existing = backend.get(k);
            if (existing != null) {
                backend.put(k, apt.mergeIntoExisting(existing));
            } else {
                backend.put(k, apt.toByteArray());
            }
        }
    }

    private AccumulativeLazyAPT getCached(int key) throws IOException {
        Object apt = cache.valAt(key);
        if (apt == null) {
            AccumulativeLazyAPT e = factory.empty();
            putCached(key, e);
            return e;
        } else {
            return (AccumulativeLazyAPT) apt;
        }
    }


    // synchronized puts into cache rather than CAS. Seems to be faster.
    private synchronized void putCached(int key, AccumulativeLazyAPT apt) throws IOException {
        cache = (APersistentMap) cache.assoc(key, apt);
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
            if (apt != null) {
                include(graph.entityIds[i], apt);
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
    public void put(Integer entityId, APT apt) {
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
}
