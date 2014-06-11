package uk.ac.susx.tag.apt;

/**
 * @author ds300
 */
public interface APTStoreBuilder<T extends APT> {
     APTStoreBuilder setBackend(PersistentKVStore<Integer, byte[]> backend);
     APTStoreBuilder setFactory(APTFactory<T> factory);
     APTStore<T> build();
}
