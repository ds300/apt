package uk.ac.susx.tag.apt.store;

import uk.ac.susx.tag.apt.APT;
import uk.ac.susx.tag.apt.APTFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Persistent storage for APTs.
 * @author ds300
 */
public interface APTStore<T extends APT> extends Closeable {
    /**
     * When there is no tree associated with {@code key}, {@code get} should return an empty
     * tree, as supplied by the factory.
     * @param key the entity ID specifying which tree should be retrieved
     * @return the tree associated with {@code key}
     * @throws java.io.IOException if storage IO fails
     */
    T get(int key) throws IOException;

    /**
     * @param key the entity ID whose existence in the store is to be determined
     * @return true if the store has an entry for {@code key}, false otherwise.
     * @throws java.io.IOException if storage IO fails
     */
    boolean has(int key) throws IOException;

    /**
     * Inserts a tree into the store, replacing any existing tree currently associated with {@code key}
     * @param key the entity ID
     * @param apt the new tree
     * @throws java.io.IOException if storage IO fails
     */
    void put(int key, T apt) throws IOException;

    /**
     * interface for constructing APTStore objects.
     */
    public interface Builder {
        /**
         * returns the APTStore, which should use the given factory for deserialization and creating empty trees.
         * @param factory the factory
         * @param <T> the APT type
         * @return the APTStore
         */
        <T extends APT> APTStore<T> build(APTFactory<T> factory);
    }
}
