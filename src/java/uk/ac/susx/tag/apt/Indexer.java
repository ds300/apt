package uk.ac.susx.tag.apt;

/**
 * Indexers assign unique integer IDs to values.
 * @author ds300
 */
public interface Indexer<V> {
    /**
     * returns an index for the given item, creating a new index if the item has never been seen before
     * @param value the value to index. null should throw a NullPointerException
     * @return the index
     */
    int getIndex(V value);

    /**
     * @param value the value to check. null should throw a NullPointerException
     * @return true if the value has been previously indexed, false otherwise.
     */
    boolean hasIndex(V value);
}
