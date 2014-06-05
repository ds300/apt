package uk.ac.susx.tag.apt.store;

import uk.ac.susx.tag.apt.APT;
import com.google.common.cache.*;
import uk.ac.susx.tag.apt.APTFactory;
import uk.ac.susx.tag.apt.Util;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author ds300
 */
public class CachedAPTStore<T extends APT> implements APTStore<T> {
    private final LoadingCache<Integer, T> trees;
    private final PersistentKVStore<Integer, byte[]> backend;
    private final APTFactory<T> factory;

    CachedAPTStore(final int maxItems, final APTFactory<T> factory, final PersistentKVStore<Integer, byte[]> backend) {
        this.factory = factory;
        this.backend = backend;
        trees = CacheBuilder.newBuilder()
                .maximumSize(maxItems)
                .removalListener(new RemovalListener<Integer, T>() {
                    @Override
                    public void onRemoval(RemovalNotification<Integer, T> notification) {

                        try {
                            backend.store(notification.getKey(), notification.getValue().toByteArray());
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
    public T get(int key) throws IOException {
        try {
            return trees.get(key);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean has(int key) throws IOException {
        return trees.asMap().containsKey(key) || backend.contains(key);
    }

    @Override
    public void put(int key, T apt) throws IOException {
        trees.put(key, apt);
    }

    public static class Builder implements APTStore.Builder {
        private int maxItems = 10000;

        private PersistentKVStore<Integer, byte[]> backend;

        public Builder setMaxItems (int maxItems) {
            if (maxItems <= 0) {
                throw new IllegalArgumentException("maxItems must be > 0");
            } else {
                this.maxItems = maxItems;
            }
            return this;
        }

        public Builder setBackend (PersistentKVStore<Integer, byte[]> backend) {
            this.backend = backend;
            return this;
        }

        @Override
        public <T extends APT> APTStore<T> build(APTFactory<T> factory) {
            if (backend == null) throw new IllegalStateException("CachedAPTStore backend not set");
            return new CachedAPTStore<>(maxItems, factory, backend);
        }
    }
}
