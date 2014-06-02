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
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author ds300
 */
public class AccumulativeDistributionalLexicon implements DistributionalLexicon<AccumulativeLazyAPT> {

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
                    if (mem() > 0.8) {
                        try {
                            invalidateAll();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        System.gc();
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


        private AtomicReference<APersistentMap> map = new AtomicReference<>(PersistentHashMap.EMPTY);

        private void invalidateAll() throws IOException {
            APersistentMap m;
            for (;;) {
                m = map.get();
                if (map.compareAndSet(m, PersistentHashMap.EMPTY)) break;
            }

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
            Object m = map.get().valAt(key);
            if (m == null) {
                AccumulativeLazyAPT e = factory.empty();
                put(key, e);
                return e;
            } else {
                return (AccumulativeLazyAPT) m;
            }
        }

        @Override
        public boolean has(int key) throws IOException {
            return map.get().containsKey(key);
        }

        @Override
        public void put(int key, AccumulativeLazyAPT apt) throws IOException {
            for (;;) {
                APersistentMap m = map.get();
                APersistentMap m2 = (APersistentMap) m.assoc(key, apt);
                if (map.compareAndSet(m, m2)) return;
            }
        }

        @Override
        public void close() throws IOException {
            invalidateAll();
            open = false;
            backend.close();
        }
    }

    private final PersistentKVStore<byte[], byte[]> backend;

    final APTStore<AccumulativeLazyAPT> store = new Store();

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

    @Override
    public void close() throws IOException {
        store.close();
    }
}
