package uk.ac.susx.tag.apt;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

/**
 * @author ds300
 */
public class Int2IntArraySortedMap extends AbstractInt2IntSortedMap {

    private final int[] keys;
    private final int[] vals;
    private final int start;
    private final int end;

    /**
     *
     * @param keys must be sorted
     * @param vals
     */
    public Int2IntArraySortedMap(int[] keys, int[] vals) {
        this.keys = keys;
        this.vals = vals;
        this.start = 0;
        this.end = keys.length;
    }

    private Int2IntArraySortedMap(int[] keys, int[] vals, int start, int end) {
        this.keys = keys;
        this.vals = vals;
        this.start = start;
        this.end = end;
    }

    private class EEntry implements Int2IntMap.Entry {
        final int k;
        final int v;

        EEntry (int idx) {
            this.k = keys[idx];
            this.v = vals[idx];
        }

        @Override
        public int getIntKey() {
            return k;
        }

        @Override
        public int setValue(int i) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getIntValue() {
            return v;
        }


        @Override
        public Integer getKey() {
            return k;
        }

        @Override
        public Integer getValue() {
            return v;
        }

        @Override
        public Integer setValue(Integer value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Int2ObjectMap.Entry
                    && ((Int2ObjectMap.Entry) o).getIntKey() == k
                    && ((Int2ObjectMap.Entry) o).getValue().equals(v);
        }

        @Override
        public int hashCode() {
            return k * 31 + v * 71;
        }
    }

    private class EIterator extends AbstractObjectBidirectionalIterator<Int2IntMap.Entry> {

        int i;

        EIterator (int i) {
            this.i = i;
        }

        @Override
        public Int2IntMap.Entry previous() {
            return i > start ? new EEntry(i--) : null;
        }

        @Override
        public boolean hasPrevious() {
            return i > start;
        }

        @Override
        public boolean hasNext() {
            return i < end;
        }

        @Override
        public Int2IntMap.Entry next() {
            return i < end ? new EEntry(i++) : null;
        }
    }

    @Override
    public ObjectSortedSet<Entry> int2IntEntrySet() {
        return new FastSortedEntrySet () {

            @Override
            public ObjectBidirectionalIterator<Entry> fastIterator(Entry entry) {
                int idx = Arrays.binarySearch(keys, start, end, entry.getIntKey());
                if (idx >= 0)
                    return new EIterator(start);
                else
                    return ObjectIterators.EMPTY_ITERATOR;
            }

            @Override
            public ObjectIterator<Entry> fastIterator() {
                return new EIterator(start);
            }

            @Override
            public ObjectBidirectionalIterator<Entry> iterator(Entry entry) {
                return fastIterator(entry);
            }

            @Override
            public ObjectBidirectionalIterator<Entry> objectIterator() {
                return new EIterator(start);
            }

            @Override
            public <T> T[] toArray(T[] ts) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean add(Entry entry) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int size() {
                return end - start;
            }

            @Override
            public boolean isEmpty() {
                return size() == 0;
            }

            @Override
            public boolean contains(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ObjectBidirectionalIterator<Entry> iterator() {
                return new EIterator(start);
            }

            @Override
            public Object[] toArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean addAll(Collection<? extends Entry> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();

            }

            @Override
            public Comparator<? super Entry> comparator() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ObjectSortedSet<Entry> subSet(Entry entry, Entry entry2) {
                return subMap(entry.getIntKey(), entry2.getIntKey()).int2IntEntrySet();
            }

            @Override
            public ObjectSortedSet<Entry> headSet(Entry entry) {
                return headMap(entry.getIntKey()).int2IntEntrySet();
            }

            @Override
            public ObjectSortedSet<Entry> tailSet(Entry entry) {
                return tailMap(entry.getIntKey()).int2IntEntrySet();
            }

            @Override
            public Entry first() {
                return iterator().next();
            }

            @Override
            public Entry last() {
                if (size() > 0) {
                    return new EEntry(end-1);
                } else {
                    return null;
                }
            }
        };
    }

    @Override
    public IntComparator comparator() {
        return null;
    }

    @Override
    public Int2IntSortedMap subMap(int lo, int hi) {
        lo = Arrays.binarySearch(keys, start, end, lo);
        if (lo < 0) lo = -(lo + 1);

        hi = Arrays.binarySearch(keys, start, end, hi);
        if (hi < 0) hi = -(hi + 1);


        if (lo == start && hi == end) {
            return this;
        } else {
            return new Int2IntArraySortedMap(keys, vals, lo, hi);
        }
    }

    @Override
    public Int2IntSortedMap headMap(int i) {
        return subMap(keys[start], i);
    }

    @Override
    public Int2IntSortedMap tailMap(int i) {
        return subMap(i, keys[end-1]);
    }

    @Override
    public int firstIntKey() {
        return keys.length > 0 ? keys[start] : 0;
    }

    @Override
    public int lastIntKey() {
        return keys.length > 0 ? keys[end-1] : 0;
    }

    @Override
    public int get(int i) {
        i = Arrays.binarySearch(keys, start, end, i);
        return i >= 0 ? vals[i] : 0;
    }

    @Override
    public int size() {
        return end-start;
    }
}
