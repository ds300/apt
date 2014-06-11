package uk.ac.susx.tag.apt;

import com.google.common.cache.*;

import java.io.*;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author ds300
 */
public class CachedAPTStore<T extends APT> implements APTStore<T> {
    private final LoadingCache<Integer, T> trees;
    private final PersistentKVStore<Integer, byte[]> backend;
    private final APTFactory<T> factory;

    public CachedAPTStore(final int maxItems, final APTFactory<T> factory, final PersistentKVStore<Integer, byte[]> backend) {
        this.factory = factory;
        this.backend = backend;
        trees = CacheBuilder.newBuilder()
                .maximumSize(maxItems)
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

    @Override
    public void include(int entityID, APT apt) throws IOException {
        trees.put(entityID, (T) get(entityID).merged(apt, Integer.MAX_VALUE));
    }

    @Override
    public void include(RGraph rGraph) throws IOException {
        ArrayAPT[] apts = ArrayAPT.factory.fromGraph(rGraph);
        int[] entityIds = rGraph.entityIds;
        for (int i=0; i<apts.length; i++) {
            ArrayAPT apt = apts[i];
            if (apt != null) {
                include(entityIds[i], apt);
            }
        }
    }


    @Override
    public void remove(Integer entityID) throws IOException {

    }

    @Override
    public boolean containsKey(Integer entityID) throws IOException {
        return trees.asMap().containsKey(entityID) || backend.containsKey(entityID);
    }

    @Override
    public void put(Integer entityID, APT apt) throws IOException {
        trees.put(entityID, (T) factory.empty().merged(apt, Integer.MAX_VALUE));
    }

}
