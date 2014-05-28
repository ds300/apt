package uk.ac.susx.tag.apt;

/**
 * Resolves an index, returning it's associated value.
 * @author ds300
 */
public interface Resolver<V> {
    /**
     * @param index the index
     * @return the value associated with the index, otherwise null
     */
    V resolve(int index);
}
