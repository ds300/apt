package uk.ac.susx.tag.apt;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.AbstractObjectBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.AbstractObjectSortedSet;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;

import java.util.Arrays;
import java.util.Comparator;


/**
 * @author ds300
 */
public class Int2ObjectArraySortedMap<V> extends AbstractInt2ObjectSortedMap<V> {
    private final int[] keys;
    private final V[] vals;
    private final int start;
    private final int end;

    /**
     * keys must be sorted and unique
     * @param keys
     * @param vals
     */
    public Int2ObjectArraySortedMap(int[] keys, V[] vals) {
        this.keys = keys;
        this.vals = vals;
        start = 0;
        end = keys.length;
    }

    private Int2ObjectArraySortedMap(int[] keys, V[] vals, int start, int end) {
        this.keys = keys;
        this.vals = vals;
        this.start = start;
        this.end = end;
    }

    private class EEntry implements Entry<V> {
        final int k;
        final V v;

        EEntry (int idx) {
            this.k = keys[idx];
            this.v = vals[idx];
        }

        @Override
        public int getIntKey() {
            return k;
        }

        @Override
        public Integer getKey() {
            return k;
        }

        @Override
        public V getValue() {
            return v;
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Entry
                    && ((Entry) o).getIntKey() == k
                    && ((Entry) o).getValue().equals(v);
        }

        @Override
        public int hashCode() {
            return k * (v == null ? 1 : v.hashCode());
        }
    }

    private class EIterator extends AbstractObjectBidirectionalIterator<Entry<V>> {

        int i;

        EIterator (int i) {
            this.i = i;
        }

        @Override
        public Entry<V> previous() {
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
        public Entry<V> next() {
            return i < end ? new EEntry(i++) : null;
        }
    }

    @Override
    public ObjectSortedSet<Entry<V>> int2ObjectEntrySet() {
        return new AbstractObjectSortedSet<Entry<V>>() {
            @Override
            public ObjectBidirectionalIterator<Entry<V>> iterator() {
                return new EIterator(start);
            }

            @Override
            public int size() {
                return keys.length;
            }

            @Override
            public ObjectBidirectionalIterator<Entry<V>> iterator(Entry<V> vEntry) {
                int idx = Arrays.binarySearch(keys, start, end, vEntry.getIntKey());
                return idx >= start ? new EIterator(idx) : null;
            }

            @Override
            public ObjectSortedSet<Entry<V>> subSet(Entry<V> vEntry, Entry<V> vEntry2) {
                return subMap(vEntry.getIntKey(), vEntry2.getIntKey()).int2ObjectEntrySet();
            }

            @Override
            public ObjectSortedSet<Entry<V>> headSet(Entry<V> vEntry) {
                return headMap(vEntry.getIntKey()).int2ObjectEntrySet();
            }

            @Override
            public ObjectSortedSet<Entry<V>> tailSet(Entry<V> vEntry) {
                return tailMap(vEntry.getIntKey()).int2ObjectEntrySet();
            }

            @Override
            public Comparator<? super Entry<V>> comparator() {
                return new Comparator<Entry<V>>() {
                    @Override
                    public int compare(Entry<V> o1, Entry<V> o2) {
                        return o1.getKey().compareTo(o2.getKey());
                    }
                };
            }

            @Override
            public Entry<V> first() {
                return new EEntry(start);
            }

            @Override
            public Entry<V> last() {
                return new EEntry(end);
            }
        };
    }

    @Override
    public IntComparator comparator() {
        return new IntComparator() {
            @Override
            public int compare(int i, int i2) {
                return i < i2 ? -1 : i == i2 ? 0 : 1;
            }

            @Override
            public int compare(Integer i, Integer i2) {
                return i.compareTo(i2);
            }
        };
    }

    @Override
    public Int2ObjectSortedMap<V> subMap(int lo, int hi) {

        lo = Arrays.binarySearch(keys, start, end, lo);
        if (lo < 0) lo = -(lo + 1);

        hi = Arrays.binarySearch(keys, start, end, hi);
        if (hi < 0) hi = -(hi + 1);


        if (lo == start && hi == end) {
            return this;
        } else {
            return new Int2ObjectArraySortedMap<>(keys, vals, lo, hi);
        }
    }

    @Override
    public Int2ObjectSortedMap<V> headMap(int hi) {
        hi = Arrays.binarySearch(keys, start, end, hi);
        if (hi < 0) hi = -(hi + 1);

        return hi == end ? this : new Int2ObjectArraySortedMap<>(keys, vals, start, hi);
    }

    @Override
    public Int2ObjectSortedMap<V> tailMap(int lo) {
        lo = Arrays.binarySearch(keys, start, end, lo);
        if (lo < 0) lo = -(lo + 1);

        return lo == start ? this : new Int2ObjectArraySortedMap<>(keys, vals, lo, end);
    }

    @Override
    public int firstIntKey() {
        return keys[start];
    }

    @Override
    public int lastIntKey() {
        return keys[end-1];
    }

    @Override
    public V get(int i) {
        int idx = Arrays.binarySearch(keys, i);
        return idx >= 0 ? vals[idx] : null;
    }

    @Override
    public int size() {
        return keys.length;
    }
}
