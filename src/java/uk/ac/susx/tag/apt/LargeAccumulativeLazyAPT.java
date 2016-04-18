package uk.ac.susx.tag.apt;

import it.unimi.dsi.fastutil.ints.*;
import pl.edu.icm.jlargearrays.ByteLargeArray;

import java.io.IOException;
import java.util.Arrays;

/**
 * Does things lazily for ultra-efficient distributional lexicon creation. This shouldn't be used manually.
 * Its doing it with _LARGE_ Arrays with lots of chest hair.
 * @author thk22
 */
public class LargeAccumulativeLazyAPT implements APT {

    static class FrozenException extends Exception {}

    static class Factory implements APTFactory<LargeAccumulativeLazyAPT> {

        @Override
        public LargeAccumulativeLazyAPT fromByteArray(byte[] bytes) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public LargeAccumulativeLazyAPT empty() {
            return new LargeAccumulativeLazyAPT();
        }

        @Override
        public LargeAccumulativeLazyAPT[] fromGraph(RGraph graph) {
            throw new UnsupportedOperationException();
        }
    }



    // use tree based maps for fast insertion when things get massive
    Int2FloatRBTreeCounter entityScores;
    Int2ObjectRBTreeMap<LargeAccumulativeLazyAPT> edges;
    private boolean frozen = false;
    private float sum = 0;

    public LargeAccumulativeLazyAPT() {
        entityScores = new Int2FloatRBTreeCounter();
        edges = new Int2ObjectRBTreeMap<>();
    }


    // returns the size in bytes that this APT will occupy on disk
    private long size() {
        long s = 16; // header
        s += entityScores.size() << 3;
        s += 4 * edges.size(); // edge labels
        for (LargeAccumulativeLazyAPT apt : edges.values()) {
            s += apt.size();
        }

        return s;
    }

    public synchronized byte[] toByteArray() {
        throw new UnsupportedOperationException("LargeAccumulativeLazyAPT does not support small native Java arrays, use AccumulativeLazyAPT instead!");
    }

    public synchronized ByteLargeArray toLargeByteArray() {
        ByteLargeArray bytes = new ByteLargeArray(size());
        toLargeByteArray(bytes, 0);
        frozen = true;
        return bytes;
    }

    private long toLargeByteArray(final ByteLargeArray bytes, final long offset) {
        // header is four integers:
        //  1.  number of bytes for token counts
        //  2.  number of bytes for children
        //  3.  number of children
        //  4.  sum of entity counts (not necessarily equal to (reduce + entityScores.values())

        Util.int2largeBytes(entityScores.size() << 3, bytes, offset);

        Util.int2largeBytes(edges.size(), bytes, offset + 8);
        Util.float2largeBytes(sum, bytes, offset + 12);

        // serialize token counts
        long i = offset + 16;
        for (Int2FloatMap.Entry entry : entityScores.int2FloatEntrySet()) {
            Util.int2largeBytes(entry.getIntKey(), bytes, i);
            Util.float2largeBytes(entry.getFloatValue(), bytes, i + 4);
            i += 8;
        }

        // now serialize children
        long j = i;
        for (Int2ObjectMap.Entry<LargeAccumulativeLazyAPT> entry : edges.int2ObjectEntrySet()) {
            // label
            Util.int2largeBytes(entry.getIntKey(), bytes, j);
            // child
            j = entry.getValue().toLargeByteArray(bytes, j + 4);
        }

        if ((long)Integer.MAX_VALUE <= (j - i)) {
            throw new ArithmeticException("Bad news, j-i > Integer.MAX_VALUE!");
        }
        int diff = (int)(j - i);

        Util.int2largeBytes(diff, bytes, offset + 4); // 2

        return j;
    }

    static class OffsetTuple {
        final long e;
        final long b;

        OffsetTuple(long e, long b) {
            this.e = e;
            this.b = b;
        }
    }

    public synchronized ByteLargeArray mergeIntoExisting (ByteLargeArray existing) {
        ByteLargeArray bytes = new ByteLargeArray(size() + existing.length()); // (probably slightly bigger than) worst-case size
        // expected = ArrayAPT.fromByteArray(existing).merged(ArrayAPT.fromByteArray(toByteArray()), Integer.MAX_VALUE).toByteArray();
        mergeIntoExisting(existing, 0, bytes, 0);
        frozen = true;
        bytes = bytes.clone();

        return bytes.clone(); // ? - No idea why it has to be copied again...
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

    private OffsetTuple mergeIntoExisting(final ByteLargeArray existing, final long existingOffset, final ByteLargeArray bytes, final long bytesOffset) {
        final long existingEntityScoreSize = Util.largeBytes2long(existing, existingOffset);
        final long existingKidsSize = Util.largeBytes2long(existing, existingOffset + 4);
        final float existingSum = Util.largeBytes2float(existing, existingOffset + 12);
        // mutable variants of offset pointers

        // declare header numbers here and serialize them at the end.
        final long tokenCountsSize;
        final long kidsSize;
        final long numKids;

        if (entityScores.size() == 0) {
            // I don't think it's possible for this branch to be taken, but better safe than sorry I s'pose.
            tokenCountsSize = existingEntityScoreSize;

            // TODO - This is possibly _VERY_VERY_ slow, improve if it becomes too annyoing
            for (long l = 0; l < tokenCountsSize + 16; l++) { // + 16 for header
                bytes.setByte(bytesOffset + l, existing.getByte(existingOffset + l));
            }
//            checkExpected(bytes, bytesOffset + 12, tokenCountsSize);
        } else {
            // merge
            long outOffset = bytesOffset + 16; // skip header

            final int numNewScores = entityScores.size();

            final long existingScoresStart = existingOffset + 16;
            final long existingCountsEnd = existingScoresStart + existingEntityScoreSize;


            // i iterates over bytes in existing
            long i = existingScoresStart;
            // j iterates over new (id, score) pairs in entityScores
            int j = 0;

            // pull new scores out of map into array for a simpler merge loop.
            int[] newEntityScores = new int[numNewScores * 2];

            for (Int2FloatMap.Entry e : entityScores.int2FloatEntrySet()) {
                newEntityScores[j++] = e.getIntKey();
                newEntityScores[j++] = Float.floatToRawIntBits(e.getFloatValue());
            }

            j = 0;


            // now iterate over arrays in parallel, merge-sort style
            while (i < existingCountsEnd && j < numNewScores) {
                long existingEntityID = Util.largeBytes2long(existing, i);
                int newEntityID = newEntityScores[j << 1];

                if (newEntityID == existingEntityID) {
                    // new scores for an entity already seen, so add the new ones to the existing ones
                    float score = Float.intBitsToFloat(newEntityScores[(j << 1) + 1]) + Util.largeBytes2float(existing, i + 4);
                    Util.int2largeBytes(newEntityID, bytes, outOffset);
                    Util.float2largeBytes(score, bytes, outOffset + 4);
//                    checkExpected(bytes, outOffset, 8);
                    outOffset += 8;
                    i += 8;
                    j++;
                } else if (newEntityID < existingEntityID) {
                    // new entity not previously seen
                    int score = newEntityScores[(j << 1) + 1];
                    Util.int2largeBytes(newEntityID, bytes, outOffset);
                    Util.int2largeBytes(score, bytes, outOffset + 4);
//                    checkExpected(bytes, outOffset, 8);
                    outOffset += 8;
                    j++;
                } else {
                    // existing entity with no new scores, just copy the bytes
                    for (long l = 0; l < 8; l++) {
                        bytes.setByte(outOffset + l, existing.getByte(i + l));
                    }
//                    checkExpected(bytes, outOffset, 8);
                    outOffset += 8;
                    i += 8;
                }
            }

            // serialize any remaining existing scores
            if (i < existingCountsEnd) {
                long numBytesRemaining = existingCountsEnd - i;
                for (long l = 0; l < numBytesRemaining; l++) {
                    bytes.setByte(outOffset + l, existing.getByte(i + l));
                }
//                checkExpected(bytes, outOffset, numBytesRemaining);
                outOffset += numBytesRemaining;
            }

            // or any remaining new scores
            while (j < numNewScores) {
                Util.int2largeBytes(newEntityScores[j << 1], bytes, outOffset);
                Util.int2largeBytes(newEntityScores[(j << 1) + 1], bytes, outOffset + 4);
//                checkExpected(bytes, outOffset, 8);
                outOffset += 8;
                j++;
            }

            tokenCountsSize = outOffset - (bytesOffset + 16);
        }

        if (edges.size() == 0) {
            // just copy the kids over.
            for (long l = 0; l < existingKidsSize; l++) {
                bytes.setByte(bytesOffset + 16 + tokenCountsSize + l, existing.getByte(existingOffset + 16 + existingEntityScoreSize + l));
            }
            kidsSize = existingKidsSize;
            numKids = Util.largeBytes2long(existing, existingOffset + 8);
//            checkExpected(bytes, bytesOffset + 12 + tokenCountsSize, existingKidsSize);

        } else {
            // merge
            int uniqueKids = 0;
            int kidsFromExisting = 0;

            final long existingKidsEnd = existingOffset + 16 + existingEntityScoreSize + existingKidsSize;

            final int[] newEdges = edges.keySet().toIntArray(); // sorted.

            // i iterates over bytes in existing
            long i = existingOffset + 16 + existingEntityScoreSize;
            // j iterates over newEdges
            int j = 0;

            long outputOffset = bytesOffset + 16 + tokenCountsSize;

            // iterate over arrays in parallel merge-sort style
            while (i < existingKidsEnd && j < newEdges.length) {
                long existingEdge = Util.largeBytes2long(existing, i);
                int newEdge = newEdges[j];

                if (existingEdge == newEdge) {
                    // existing edge and new edge, so they need merging.
                    Util.long2largeBytes(existingEdge, bytes, outputOffset); // write out label
//                    checkExpected(bytes, outputOffset, 4);
                    OffsetTuple t = edges.get(existingEdge).mergeIntoExisting(existing, i + 4, bytes, outputOffset + 4);
//                    checkExpected(bytes, outputOffset, t.b - outputOffset);
                    i = t.e;
                    outputOffset = t.b;
                    j++;
                    kidsFromExisting++;
                } else if (existingEdge < newEdge) {
                    // calculate size of kid (including edge label) so we can just use System.arrayCopy
                    long storedKidSize = Util.largeBytes2long(existing, i+4) //tknscores size
                            + Util.largeBytes2long(existing, i+8) // kids size
                            + 16 // header
                            + 4; // edge label
                    for (long l = 0; l < storedKidSize; l++) {
                        bytes.setByte(outputOffset + l, existing.getByte(i + l));
                    }
//                    checkExpected(bytes, outputOffset, storedKidSize);
                    i += storedKidSize;
                    outputOffset += storedKidSize;
                    kidsFromExisting++;
                } else {
                    // new edge, so just write out label and then .toByteArray
                    Util.long2largeBytes(newEdge, bytes, outputOffset);
//                    checkExpected(bytes, outputOffset, 4);
//                    int x = outputOffset + 4;
                    outputOffset = edges.get(newEdge).toLargeByteArray(bytes, outputOffset + 4);
//                    checkExpected(bytes, x, outputOffset - x);
                    j++;
                }
                uniqueKids++;
            }

            // copy over any remaining existing edges
            if (i < existingKidsEnd) {
                long remainingBytes = existingKidsEnd - i;
                for (long l = 0; l < remainingBytes; l++) {
                    bytes.setByte(outputOffset + l, existing.getByte(i + l));
                }
//                checkExpected(bytes, outputOffset, remainingBytes);
                outputOffset += remainingBytes;
                uniqueKids += Util.largeBytes2long(existing, existingOffset + 8) - kidsFromExisting;
            }

            // or serialize any remaining new edges
            while (j < newEdges.length) {
                int newEdge = newEdges[j];
                Util.long2largeBytes(newEdge, bytes, outputOffset);
//                checkExpected(bytes, outputOffset, 4);
//                int x = outputOffset + 4;
                outputOffset = edges.get(newEdge).toLargeByteArray(bytes, outputOffset + 4);
//                checkExpected(bytes, x, outputOffset - x);
                j++;
                uniqueKids++;
            }

            kidsSize = outputOffset - (tokenCountsSize + 16 + bytesOffset);
            numKids = uniqueKids;
        }

        // write out header
        Util.long2largeBytes(tokenCountsSize, bytes, bytesOffset);
        Util.long2largeBytes(kidsSize, bytes, bytesOffset + 4);
        Util.long2largeBytes(numKids, bytes, bytesOffset + 8);
        Util.float2largeBytes(existingSum + sum, bytes, bytesOffset + 12);

//        checkExpected(bytes, bytesOffset, 12);

        return new OffsetTuple(existingOffset + 16 + existingKidsSize + existingEntityScoreSize, bytesOffset + 16 + tokenCountsSize + kidsSize);

    }

    public synchronized void merge (APT other, int depth, int returnPath) throws FrozenException {

        if (frozen) throw new FrozenException();


        if (other instanceof ArrayAPT) {
            mergeArrayAPT((ArrayAPT) other, depth, returnPath);
        } else {

            sum += other.sum();

            for (Int2FloatMap.Entry e : other.entityScores().int2FloatEntrySet()) {
                entityScores.increment(e.getIntKey(), e.getFloatValue());
            }

            if (depth > 0) {
                for (Int2ObjectMap.Entry<APT> e : other.edges().int2ObjectEntrySet()) {
                    int edge = e.getIntKey();
                    if (edge != returnPath) {
                        LargeAccumulativeLazyAPT existing = edges.get(e.getIntKey());
                        if (existing == null) {
                            existing = new LargeAccumulativeLazyAPT();
                            edges.put(e.getIntKey(), existing);
                        }
                        existing.merge(e.getValue(), depth-1, -edge);
                    }
                }
            }
        }
    }

    private void mergeArrayAPT(ArrayAPT other, int depth, int returnPath) {
        sum += other.sum();
        int[] entities = other.entities;
        float[] scores = other.scores;
        for (int i=0; i < entities.length; i++)
            entityScores.increment(entities[i], scores[i]);

        if (depth > 0) {
            int[] oedges = other.edges;
            ArrayAPT[] okids = other.kids;
            for (int i=0; i<oedges.length; i++) {
                int edge = oedges[i];
                if (edge != returnPath) {
                    LargeAccumulativeLazyAPT existing = edges.get(edge);
                    if (existing == null) {
                        existing = new LargeAccumulativeLazyAPT();
                        edges.put(edge, existing);
                    }
                    existing.mergeArrayAPT(okids[i], depth-1, -edge);
                }
            }
        }
    }

    //todo: decide whether or not it's worth keeping this as an implementer of APT interface

    @Override
    public APT getChild(int relation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getScore(int entityId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float sum() {
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
    public Int2FloatSortedMap entityScores() {
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
    public void walk(APTVisitor visitor, int depth) {
        throw new UnsupportedOperationException();
    }

    @Override
    public APT withScore(int tokenId, float score) {
        throw new UnsupportedOperationException();
    }

    @Override
    public APT withIncrementedScore(int tokenId, float score) {
        throw new UnsupportedOperationException();
    }

    @Override
    public APT withEdge(int dependencyId, APT child) {
        throw new UnsupportedOperationException();
    }
}
