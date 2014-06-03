package uk.ac.susx.tag.apt.construct;

import clojure.lang.APersistentMap;
import clojure.lang.PersistentHashMap;
import uk.ac.susx.tag.apt.APT;
import uk.ac.susx.tag.apt.APTFactory;
import uk.ac.susx.tag.apt.DistributionalLexicon;
import uk.ac.susx.tag.apt.Util;
import uk.ac.susx.tag.apt.store.APTStore;
import uk.ac.susx.tag.apt.store.PersistentKVStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author ds300
 */
public class AccumulativeDistributionalLexicon implements DistributionalLexicon<AccumulativeLazyAPT> {

    double memoryFullThreshold = 0.8;

    private class Store implements APTStore<AccumulativeLazyAPT> {
        boolean open = true;

        Thread memoryWatcher = new Thread() {
            Runtime rt = Runtime.getRuntime();
            double mem() {
                return (double) (rt.totalMemory() - rt.freeMemory()) / rt.maxMemory();
            }
            @Override
            public void run(){
                while (open) {
                    if (mem() > memoryFullThreshold) {
                        try {
                            System.out.println("invalidating: " + mem());
                            invalidateAll();
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


        private APersistentMap map = PersistentHashMap.EMPTY;

        private synchronized void invalidateAll() throws IOException {
            System.out.println("invalidating...");
            System.out.flush();
            APersistentMap m = map;
            map = PersistentHashMap.EMPTY;

            byte[] key = new byte[4];

            for (Object o : m.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                int k = (int) e.getKey();
                AccumulativeLazyAPT apt = (AccumulativeLazyAPT) e.getValue();
                Util.int2bytes(k, key, 0);
                byte[] existing = backend.get(key);
                if (existing != null) {
                    backend.store(key, apt.mergeIntoExisting(existing));
                } else {
                    backend.store(key, apt.toByteArray());
                }
            }
        }

        @Override
        public AccumulativeLazyAPT get(int key) throws IOException {
            Object apt = map.valAt(key);
            if (apt == null) {
                AccumulativeLazyAPT e = factory.empty();
                put(key, e);
                return e;
            } else {
                return (AccumulativeLazyAPT) apt;
            }
        }

        @Override
        public boolean has(int key) throws IOException {
            return map.containsKey(key);
        }

        @Override
        public synchronized void put(int key, AccumulativeLazyAPT apt) throws IOException {
            map = (APersistentMap) map.assoc(key, apt);
        }

        @Override
        public void close() throws IOException {
            invalidateAll();
            open = false;
            backend.close();
        }
    }

    private final PersistentKVStore<byte[], byte[]> backend;

    final Store store = new Store();

    final APTFactory<AccumulativeLazyAPT> factory = new AccumulativeLazyAPT.Factory();
    final int depth;

    public AccumulativeDistributionalLexicon(PersistentKVStore<byte[], byte[]> backend, int depth) {
        this.backend = backend;
        this.depth = depth;
    }

    @Override
    public void include(int entityId, APT apt) throws IOException {
        for (;;) {
            try {
                AccumulativeLazyAPT existing = store.get(entityId);
                existing.merge(apt, depth, 0);
                store.put(entityId, existing);
                return;
            } catch (FrozenException ignored) {
            }
        }
    }

    public AccumulativeDistributionalLexicon setMemoryFullThreshold(double d) {
        if (d <=0 || d >= 1) throw new IllegalArgumentException("d must be between 0 and 1 exclusive");

        memoryFullThreshold = d;
        return this;
    }


    @Override
    public AccumulativeLazyAPT getAPT(int entityId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replace(int entityId, APT apt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(int entityId) {
        throw new UnsupportedOperationException();
    }

    boolean closed = false;

    private void assertNotClosed() {
        if (closed) throw new IllegalStateException("Attempting to use lexicon after close");
    }

    @Override
    public void close() throws IOException {
        store.close();
        try {
            store.memoryWatcher.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        closed = true;
    }
}
