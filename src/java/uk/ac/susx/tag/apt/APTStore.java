package uk.ac.susx.tag.apt;

import java.io.IOException;

/**
 * An interface for persistent APT storage
 * @author ds300
 */
public interface APTStore<T extends APT> extends PersistentKVStore<Integer, T> {
    /**
     * Includes the given APT in the store, merging into any existing APT for the given ID
     * @param entityID
     * @param apt
     * @throws IOException
     */
    void include(int entityID, APT apt) throws IOException;



}
