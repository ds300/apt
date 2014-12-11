package uk.ac.susx.tag.apt;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * @author ds300
 */
public interface PersistentKVStore<K, V> extends Closeable, Iterable<Map.Entry<K, V>> {
    void put(K key, V value) throws IOException;
    void remove(K key) throws IOException;
    V get(K key) throws IOException;
    boolean containsKey(K key) throws IOException;
}
