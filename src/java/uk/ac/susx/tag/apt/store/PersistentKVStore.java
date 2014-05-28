package uk.ac.susx.tag.apt.store;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author ds300
 */
public interface PersistentKVStore<K, V> extends Closeable {
    void store(K key, V value) throws IOException;
    V get(K key) throws IOException;
    boolean contains(K key) throws IOException;
}
