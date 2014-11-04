package uk.ac.susx.tag.apt;

import java.util.Iterator;

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

    /**
     * @return an iteralbe of the indices that this resolver knows about
     */
    Iterable<Integer> getIndices();
}
