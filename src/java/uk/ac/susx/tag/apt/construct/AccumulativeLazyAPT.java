package uk.ac.susx.tag.apt.construct;

import it.unimi.dsi.fastutil.ints.*;
import uk.ac.susx.tag.apt.*;

import java.io.*;
import java.util.Arrays;

/**
 * Does things lazily for ultra-efficient distributional lexicon creation. This shouldn't be used manually.
 * @author ds300
 */
public class AccumulativeLazyAPT implements APT {

    static class Factory implements APTFactory<AccumulativeLazyAPT> {

        @Override
        public AccumulativeLazyAPT fromByteArray(byte[] bytes) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public AccumulativeLazyAPT empty() {
            return new AccumulativeLazyAPT();
        }

        @Override
        public AccumulativeLazyAPT[] fromGraph(RGraph graph) {
            throw new UnsupportedOperationException();
        }
    }



    // use tree based maps for fast insertion when things get massive
    Int2IntRBTreeCounter tokenCounts;
    Int2ObjectRBTreeMap<AccumulativeLazyAPT> edges;
    private boolean frozen = false;

    public AccumulativeLazyAPT() {
        tokenCounts = new Int2IntRBTreeCounter();
        edges = new Int2ObjectRBTreeMap<>();
    }

    private int size() {
        int s = 8 + 4; // header + numkids
        s += tokenCounts.size() << 3;
        s += 4 * edges.size(); // edge labels
        for (AccumulativeLazyAPT apt : edges.values()) {
            s += apt.size();
        }
        return s;
    }

    public synchronized byte[] toByteArray() {
        byte[] bytes = new byte[size()];
        toByteArray(bytes, 0);
        frozen = true;
        return bytes;
    }

    private int toByteArray(final byte[] bytes, final int offset) {
        // header is three integers:
        //  1.  number of bytes for token counts
        //  2.  number of bytes for children
        //  3.  number of children

        Util.int2bytes(tokenCounts.size() << 3, bytes, offset); // 1
        // 2 calculated later
        Util.int2bytes(edges.size(), bytes, offset + 8); // 3

        // then serialize token counts
        int i = offset + 12;
        for (Int2IntMap.Entry entry : tokenCounts.int2IntEntrySet()) {
            Util.int2bytes(entry.getIntKey(), bytes, i);
            Util.int2bytes(entry.getIntValue(), bytes, i+4);
            i+=8;
        }

        // now serialize children
        int j = i;
        for (Int2ObjectMap.Entry<AccumulativeLazyAPT> entry : edges.int2ObjectEntrySet()) {
            // label
            Util.int2bytes(entry.getIntKey(), bytes, j);
            // child
            j = entry.getValue().toByteArray(bytes, j+4);
        }

        Util.int2bytes(j - i, bytes, offset + 4); // 2

        return j;
    }

    static class OffsetTuple {
        final int e;
        final int b;

        OffsetTuple(int e, int b) {
            this.e = e;
            this.b = b;
        }
    }

    synchronized byte[] mergeIntoExisting (byte[] existing) {
        byte[] bytes = new byte[size() + existing.length]; // (probably slightly bigger than) worst-case size
//        expected = ArrayAPT.fromByteArray(existing).merged(ArrayAPT.fromByteArray(toByteArray()), Integer.MAX_VALUE).toByteArray();
        int i = mergeIntoExisting(existing, 0, bytes, 0).b;
        frozen = true;
        bytes = Arrays.copyOf(bytes, i);

        return Arrays.copyOf(bytes, i);
    }

//    static byte[] expected;



//    public static void checkExpected(byte[] bytes, int offset, int length) {
//        for (int i = offset; i < offset + length; i++)
//            if (bytes[i] != expected[i]) {
//                byte[] ex = new byte[length];
//                byte[] ac = new byte[length];
//                System.arraycopy(expected, offset, ex, 0, length);
//                System.arraycopy(bytes, offset, ac, 0, length);
//
//                System.out.println(Arrays.toString(ex));
//                System.out.println(Arrays.toString(ac));
//                throw new RuntimeException("not equals");
//            }
//    }

    private OffsetTuple mergeIntoExisting(final byte[] existing, final int existingOffset, final byte[] bytes, final int bytesOffset) {
        final int existingTokenCountSize = Util.bytes2int(existing, existingOffset);
        final int existingKidsSize = Util.bytes2int(existing, existingOffset + 4);
        // mutable variants of offset pointers

        // declare header numbers here and serialize them at the end.
        final int tokenCountsSize;
        final int kidsSize;
        final int numKids;

        if (tokenCounts.size() == 0) {
            // I don't think it's possible for this branch to be taken, but better safe than sorry I s'pose.
            tokenCountsSize = existingTokenCountSize;
            System.arraycopy(existing, existingOffset, bytes, bytesOffset, tokenCountsSize + 12);  // + 12 for header
//            checkExpected(bytes, bytesOffset + 12, tokenCountsSize);
        } else {
            // merge
            int outOffset = bytesOffset + 12; // skip header

            final int numNewCounts = tokenCounts.size();

            final int existingCountsStart = existingOffset + 12;
            final int existingCountsEnd = existingCountsStart + existingTokenCountSize;


            // i iterates over bytes in existing
            int i = existingCountsStart;
            // j iterates over new (id, count) pairs in tokenCounts
            int j = 0;

            // pull new counts out of map into array for a simpler merge loop.
            int[] newTokenCounts = new int[numNewCounts * 2];

            for (Int2IntMap.Entry e : tokenCounts.int2IntEntrySet()) {
                newTokenCounts[j++] = e.getIntKey();
                newTokenCounts[j++] = e.getIntValue();
            }

            j = 0;


            // now iterate over arrays in parallel, merge-sort style
            while (i < existingCountsEnd && j < numNewCounts) {
                int existingEntityID = Util.bytes2int(existing, i);
                int newEntityID = newTokenCounts[j << 1];

                if (newEntityID == existingEntityID) {
                    // new counts for an entity already seen, so add the new ones to the existing ones
                    int count = newTokenCounts[(j << 1) + 1] + Util.bytes2int(existing, i + 4);
                    Util.int2bytes(newEntityID, bytes, outOffset);
                    Util.int2bytes(count, bytes, outOffset+4);
//                    checkExpected(bytes, outOffset, 8);
                    outOffset += 8;
                    i += 8;
                    j++;
                } else if (newEntityID < existingEntityID) {
                    // new entity not previously seen
                    int count = newTokenCounts[(j << 1) + 1];
                    Util.int2bytes(newEntityID, bytes, outOffset);
                    Util.int2bytes(count, bytes, outOffset+4);
//                    checkExpected(bytes, outOffset, 8);
                    outOffset += 8;
                    j++;
                } else {
                    // existing entity with no new counts, just copy the bytes
                    System.arraycopy(existing, i, bytes, outOffset, 8);
//                    checkExpected(bytes, outOffset, 8);
                    outOffset += 8;
                    i += 8;
                }
            }

            // serialize any remaining existing counts
            if (i < existingCountsEnd) {
                int numBytesRemaining = existingCountsEnd - i;
                System.arraycopy(existing, i, bytes, outOffset, numBytesRemaining);
//                checkExpected(bytes, outOffset, numBytesRemaining);
                outOffset += numBytesRemaining;
            }

            // or any remaining new counts
            while (j < numNewCounts) {
                Util.int2bytes(newTokenCounts[j << 1], bytes, outOffset);
                Util.int2bytes(newTokenCounts[(j << 1) + 1], bytes, outOffset + 4);
//                checkExpected(bytes, outOffset, 8);
                outOffset += 8;
                j++;
            }

            tokenCountsSize = outOffset - (bytesOffset + 12);
        }

        if (edges.size() == 0) {
            // just copy the kids over.
            System.arraycopy(existing, existingOffset + 12 + existingTokenCountSize, bytes, bytesOffset + 12 + tokenCountsSize, existingKidsSize);
            kidsSize = existingKidsSize;
            numKids = Util.bytes2int(existing, existingOffset + 8);
//            checkExpected(bytes, bytesOffset + 12 + tokenCountsSize, existingKidsSize);

        } else {
            // merge
            int uniqueKids = 0;
            int kidsFromExisting = 0;

            final int existingKidsEnd = existingOffset + 12 + existingTokenCountSize + existingKidsSize;

            final int[] newEdges = edges.keySet().toIntArray(); // sorted.

            // i iterates over bytes in existing
            int i = existingOffset + 12 + existingTokenCountSize;
            // j iterates over newEdges
            int j = 0;

            int outputOffset = bytesOffset + 12 + tokenCountsSize;

            // iterate over arrays in parallel merge-sort style
            while (i < existingKidsEnd && j < newEdges.length) {
                int existingEdge = Util.bytes2int(existing, i);
                int newEdge = newEdges[j];

                if (existingEdge == newEdge) {
                    // existing edge and new edge, so they need merging.
                    Util.int2bytes(existingEdge, bytes, outputOffset); // write out label
//                    checkExpected(bytes, outputOffset, 4);
                    OffsetTuple t = edges.get(existingEdge).mergeIntoExisting(existing, i + 4, bytes, outputOffset + 4);
//                    checkExpected(bytes, outputOffset, t.b - outputOffset);
                    i = t.e;
                    outputOffset = t.b;
                    j++;
                    kidsFromExisting++;
                } else if (existingEdge < newEdge) {
                    // calculate size of kid (including edge label) so we can just use System.arrayCopy
                    int storedKidSize = Util.bytes2int(existing, i+4) //tkncounts size
                            + Util.bytes2int(existing, i+8) // kids size
                            + 12 // header
                            + 4; // edge label
                    System.arraycopy(existing, i, bytes, outputOffset, storedKidSize); // + 4 for edge label
//                    checkExpected(bytes, outputOffset, storedKidSize);
                    i += storedKidSize;
                    outputOffset += storedKidSize;
                    kidsFromExisting++;
                } else {
                    // new edge, so just write out label and then .toByteArray
                    Util.int2bytes(newEdge, bytes, outputOffset);
//                    checkExpected(bytes, outputOffset, 4);
//                    int x = outputOffset + 4;
                    outputOffset = edges.get(newEdge).toByteArray(bytes, outputOffset + 4);
//                    checkExpected(bytes, x, outputOffset - x);
                    j++;
                }
                uniqueKids++;
            }

            // copy over any remaining existing edges
            if (i < existingKidsEnd) {
                int remainingBytes = existingKidsEnd - i;
                System.arraycopy(existing, i, bytes, outputOffset, remainingBytes);
//                checkExpected(bytes, outputOffset, remainingBytes);
                outputOffset += remainingBytes;
                uniqueKids += Util.bytes2int(existing, existingOffset + 8) - kidsFromExisting;
            }

            // or serialize any remaining new edges
            while (j < newEdges.length) {
                int newEdge = newEdges[j];
                Util.int2bytes(newEdge, bytes, outputOffset);
//                checkExpected(bytes, outputOffset, 4);
                int x = outputOffset + 4;
                outputOffset = edges.get(newEdge).toByteArray(bytes, outputOffset + 4);
//                checkExpected(bytes, x, outputOffset - x);
                j++;
                uniqueKids++;
            }

            kidsSize = outputOffset - (tokenCountsSize + 12 + bytesOffset);
            numKids = uniqueKids;
        }

        // write out header
        Util.int2bytes(tokenCountsSize, bytes, bytesOffset);
        Util.int2bytes(kidsSize, bytes, bytesOffset + 4);
        Util.int2bytes(numKids, bytes, bytesOffset + 8);

//        checkExpected(bytes, bytesOffset, 12);

        return new OffsetTuple(existingOffset + 12 + existingKidsSize + existingTokenCountSize, bytesOffset + 12 + tokenCountsSize + kidsSize);
    }

    synchronized void merge (APT other, int depth, int returnPath) throws FrozenException {

        if (frozen) throw new FrozenException();

        for (Int2IntMap.Entry e : other.entityCounts().int2IntEntrySet()) {
            tokenCounts.increment(e.getIntKey(), e.getIntValue());
        }

        if (depth > 0) {
            for (Int2ObjectMap.Entry<APT> e : other.edges().int2ObjectEntrySet()) {
                int edge = e.getIntKey();
                if (edge != returnPath) {
                    AccumulativeLazyAPT existing = edges.get(e.getIntKey());
                    if (existing == null) {
                        existing = new AccumulativeLazyAPT();
                        edges.put(e.getIntKey(), existing);
                    }
                    existing.merge(e.getValue(), depth-1, -edge);
                }
            }
        }

    }


    @Override
    public APT getChild(int relation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getCount(int entityId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int sum() {
        throw new UnsupportedOperationException();
    }

    @Override
    public APT merged(APT other, int depth) {
        throw new UnsupportedOperationException();
    }

    @Override
    public APT culled(int depth) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Int2IntSortedMap entityCounts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Int2ObjectSortedMap<APT> edges() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void walk(APTVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public APT withCount(int tokenId, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public APT withEdge(int dependencyId, APT child) {
        throw new UnsupportedOperationException();
    }
}
