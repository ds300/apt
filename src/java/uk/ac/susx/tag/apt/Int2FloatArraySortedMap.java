package uk.ac.susx.tag.apt;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

/**
 * @author ds300
 */
public class Int2FloatArraySortedMap extends AbstractInt2FloatSortedMap {

    final int[] keys;
    final float[] vals;
    final int start;
    final int end;

    private static void assertSorted(int[] ks) {
        int max = Integer.MIN_VALUE;
        for (int k : ks)
            if (k < max) throw new RuntimeException("Key array not sorted");
            else max = k;
    }

    /**
     *
     * @param keys must be sorted in ascending order
     * @param vals
     */
    public Int2FloatArraySortedMap(int[] keys, float[] vals) {
        assertSorted(keys);
        this.keys = keys;
        this.vals = vals;
        this.start = 0;
        this.end = keys.length;
    }

    private Int2FloatArraySortedMap(int[] keys, float[] vals, int start, int end) {
        this.keys = keys;
        this.vals = vals;
        this.start = start;
        this.end = end;
    }

    private class EEntry implements Entry {
        final int k;
        final float v;

        EEntry (int idx) {
            this.k = keys[idx];
            this.v = vals[idx];
        }

        @Override
        public int getIntKey() {
            return k;
        }

        @Override
        public float setValue(float i) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloatValue() {
            return v;
        }


        @Override
        public Integer getKey() {
            return k;
        }

        @Override
        public Float getValue() {
            return v;
        }

        @Override
        public Float setValue(Float value) {
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
            return (int) (k * 31 + v * 71);
        }
    }

    private class EIterator extends AbstractObjectBidirectionalIterator<Entry> {

        int i;

        EIterator (int i) {
            this.i = i;
        }

        @Override
        public Entry previous() {
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
        public Entry next() {
            return i < end ? new EEntry(i++) : null;
        }
    }

    @Override
    public ObjectSortedSet<Entry> int2FloatEntrySet() {
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
                return subMap(entry.getIntKey(), entry2.getIntKey()).int2FloatEntrySet();
            }

            @Override
            public ObjectSortedSet<Entry> headSet(Entry entry) {
                return headMap(entry.getIntKey()).int2FloatEntrySet();
            }

            @Override
            public ObjectSortedSet<Entry> tailSet(Entry entry) {
                return tailMap(entry.getIntKey()).int2FloatEntrySet();
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
    public Int2FloatSortedMap subMap(int lo, int hi) {
        lo = Arrays.binarySearch(keys, start, end, lo);
        if (lo < 0) lo = -(lo + 1);

        hi = Arrays.binarySearch(keys, start, end, hi);
        if (hi < 0) hi = -(hi + 1);


        if (lo == start && hi == end) {
            return this;
        } else {
            return new Int2FloatArraySortedMap(keys, vals, lo, hi);
        }
    }

    @Override
    public Int2FloatSortedMap headMap(int i) {
        return subMap(keys[start], i);
    }

    @Override
    public Int2FloatSortedMap tailMap(int i) {
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
    public float get(int i) {
        i = Arrays.binarySearch(keys, start, end, i);
        return i >= 0 ? vals[i] : 0;
    }

    @Override
    public int size() {
        return end-start;
    }

    public int[] keys() {
        return Arrays.copyOf(keys, keys.length);
    }

    public float[] vals() {
        return Arrays.copyOf(vals, vals.length);
    }
}
