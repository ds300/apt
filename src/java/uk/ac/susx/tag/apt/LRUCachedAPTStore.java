package uk.ac.susx.tag.apt;

import com.google.common.cache.*;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author ds300
 */
public class LRUCachedAPTStore<T extends APT> implements PersistentKVStore<Integer, T> {
    private final LoadingCache<Integer, T> trees;
    private final PersistentKVStore<Integer, byte[]> backend;
    private final APTFactory<T> factory;
    private final int depth;

    private LRUCachedAPTStore(final int maxItems, final int depth, final APTFactory<T> factory, final PersistentKVStore<Integer, byte[]> backend) {
        this.factory = factory;
        this.backend = backend;
        this.depth = depth;
        trees = CacheBuilder.newBuilder()
                .maximumSize(maxItems)
                .softValues()
                .removalListener(new RemovalListener<Integer, T>() {
                    @Override
                    public void onRemoval(RemovalNotification<Integer, T> notification) {

                        try {
                            backend.put(notification.getKey(), notification.getValue().toByteArray());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                    }
                }).build(new CacheLoader<Integer, T>() {
                    @Override
                    public T load(Integer integer) throws Exception {
                        byte[] loaded = backend.get(integer);
                        if (loaded != null) {
                            return factory.fromByteArray(loaded);
                        } else {
                            return factory.empty();
                        }
                    }
                });
    }

    @Override
    public void close() throws IOException {
        trees.invalidateAll();
        backend.close();
    }

    @Override
    public T get(Integer key) throws IOException {
        try {
            return trees.get(key);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

//    public void include(int entityID, APT apt) throws IOException {
//        trees.put(entityID, (T) get(entityID).merged(apt, depth));
//    }
//
//    public void include(RGraph rGraph) throws IOException {
//        ArrayAPT[] apts = ArrayAPT.factory.fromGraph(rGraph);
//        int[] entityIds = rGraph.entityIds;
//        for (int i=0; i<apts.length; i++) {
//            ArrayAPT apt = apts[i];
//            if (apt != null) {
//                include(entityIds[i], apt);
//            }
//        }
//    }


    @Override
    public synchronized boolean atomicCAS(Integer key, T expected, T value) throws IOException {
        T existing = get(key);
        if (existing.equals(expected)) {
            put(key, value);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized void remove(Integer entityID) throws IOException {
        trees.invalidate(entityID);
        backend.remove(entityID);
    }

    @Override
    public boolean containsKey(Integer entityID) throws IOException {
        return trees.asMap().containsKey(entityID) || backend.containsKey(entityID);
    }

    @Override
    public synchronized void put(Integer entityID, APT apt) throws IOException {
        trees.put(entityID, (T) factory.empty().merged(apt, Integer.MAX_VALUE));
    }

    private class Entry implements Map.Entry<Integer, T> {
        final int key;
        final T val;

        private Entry(int key, T val) {
            this.key = key;
            this.val = val;
        }

        @Override
        public Integer getKey() {
            return key;
        }

        @Override
        public T getValue() {
            return val;
        }

        @Override
        public T setValue(T value) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Iterator<Map.Entry<Integer, T>> iterator() {
        final Iterator<Map.Entry<Integer, byte[]>> delegate = backend.iterator();
        return new Iterator<Map.Entry<Integer, T>>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Map.Entry<Integer, T> next() {
                Map.Entry<Integer, byte[]> e = delegate.next();
                if (e == null)
                    return null;
                else {
                    try {
                        return new Entry(e.getKey(), factory.fromByteArray(e.getValue()));
                    } catch (IOException e1) {
                        throw new RuntimeException(e1);
                    }
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static class Builder<T extends APT> {

        PersistentKVStore<Integer, byte[]> backend;
        APTFactory<T> factory;
        int depth = 2;
        int maxItems = 10000;

        public Builder<T> setBackend(PersistentKVStore<Integer, byte[]> backend) {
            this.backend = backend;
            return this;
        }

        public Builder<T> setFactory(APTFactory<T> factory) {
            this.factory = factory;
            return this;
        }

        public Builder<T> setMaxDepth(int depth) {
            this.depth = depth;
            return this;
        }

        public Builder<T> setMaxItems(int maxItems) {
            this.maxItems = maxItems;
            return this;
        }

        public LRUCachedAPTStore<T> build() {
            if (backend == null) throw new UnsupportedOperationException("CachedAPTStore requires a backend");
            if (factory == null) throw new UnsupportedOperationException("CachedAPTStore requires a factory");
            else return new LRUCachedAPTStore<>(maxItems, depth, factory, backend);
        }
    }
}
