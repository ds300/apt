package uk.ac.susx.tag.apt.construct;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import uk.ac.susx.tag.apt.*;

import java.io.*;

/**
 * Does things lazily for ultra-efficient distributional lexicon creation. This shouldn't be used manually.
 * @author ds300
 */
public class AccumulativeLazyAPT implements APT {

    static class Factory implements APTFactory<AccumulativeLazyAPT> {

        @Override
        public AccumulativeLazyAPT read(InputStream inputStream) throws IOException {
            return fromInputStream(inputStream);
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

    // store bytes here for lazy stuff
    byte[] childrenBytes;

    // don't populate the map with token counts from disk. We only need to read them when re-serializing
    byte[] tokenCountBytes;

    // use tree based maps for fast insertion when things get massive
    Int2IntRBTreeCounter tokenCounts;
    Int2ObjectRBTreeMap<AccumulativeLazyAPT> edges;

    public AccumulativeLazyAPT() {
        tokenCounts = new Int2IntRBTreeCounter();
        edges = new Int2ObjectRBTreeMap<>();
    }

    private AccumulativeLazyAPT(byte[] childrenBytes, byte[] tokenCountBytes) {
        this.childrenBytes = childrenBytes;
        this.tokenCountBytes = tokenCountBytes;
    }

    private void ensureTokenCounts () {
        if (tokenCounts == null) {
            tokenCounts = new Int2IntRBTreeCounter();
        }
    }

    private void ensureEdges() {
        if (childrenBytes != null) {
            edges = new Int2ObjectRBTreeMap<>();
            int numChildren = Util.bytes2int(childrenBytes, 0);
            final byte[] buf = new byte[4];
            ByteArrayInputStream in = new ByteArrayInputStream(childrenBytes, 4, childrenBytes.length - 4);
            for (int i=0; i<numChildren; i++) {
                in.read(buf, 0, 4);
                int edge = Util.bytes2int(buf);
                try {
                    AccumulativeLazyAPT child = fromInputStream(in);
                    edges.put(edge, child);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            childrenBytes = null;
        }
    }

    synchronized void merge (APT other, int depth, int returnPath) {
        ensureTokenCounts();
        for (Int2IntMap.Entry e : other.entityCounts().int2IntEntrySet()) {
            tokenCounts.increment(e.getIntKey(), e.getIntValue());
        }

        if (depth > 0) {
            ensureEdges();
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

    public static AccumulativeLazyAPT fromInputStream (InputStream in) throws IOException {
        byte[] header = new byte[8];
        if (Util.read(in, header, 0, 8) != 8) throw new IOException("unexpected end of stream");
        int numTokenBytes = Util.bytes2int(header, 0);
        int numChildrenBytes = Util.bytes2int(header, 4);
        byte[] tokenBytes = new byte[numTokenBytes];
        byte[] childrenBytes = new byte[numChildrenBytes];
        if (Util.read(in, tokenBytes, 0, numTokenBytes) != numTokenBytes) throw new IOException("unexpected end of stream");
        if (Util.read(in, childrenBytes, 0, numChildrenBytes) != numChildrenBytes) throw new IOException("unexpected end of stream");
        return new AccumulativeLazyAPT(childrenBytes, tokenBytes);
    }

    @Override
    public synchronized void writeTo(OutputStream out) throws IOException {
        byte[] header = new byte[8];

        byte[] serializedTokenCounts;
        int serializedTokenCountsSize;

        if (tokenCounts == null) {
            serializedTokenCounts = tokenCountBytes == null ? new byte[0] : tokenCountBytes;
            serializedTokenCountsSize = serializedTokenCounts.length;
        } else if (tokenCountBytes != null && tokenCountBytes.length > 0 && !tokenCounts.isEmpty()) {
            // worst-case size
            serializedTokenCounts = new byte[tokenCountBytes.length + (tokenCounts.size() << 3)];

            // merge tokenCounts and tokenCountBytes
            final int numStoredCounts = tokenCountBytes.length >>> 3;
            final int numNewCounts = tokenCounts.size();

            int i = 0;
            int j = 0;
            int x = 0;

            int[] newTokenCounts = new int[numNewCounts * 2];

            for (Int2IntMap.Entry e : tokenCounts.int2IntEntrySet()) {
                newTokenCounts[j++] = e.getIntKey();
                newTokenCounts[j++] = e.getIntValue();
            }

            j = 0;



            while (i < numStoredCounts && j < numNewCounts) {
                int storedEntityID = Util.bytes2int(tokenCountBytes, i << 3);
                int newEntityID = newTokenCounts[j << 1];

                if (newEntityID == storedEntityID) {
                    int count = newTokenCounts[(j << 1) + 1] + Util.bytes2int(tokenCountBytes, (i<<3) + 4);
                    Util.int2bytes(newEntityID, serializedTokenCounts, x);
                    Util.int2bytes(count, serializedTokenCounts, x+4);
                    x += 8;
                    j++;
                    i++;
                } else if (newEntityID < storedEntityID) {
                    int count = newTokenCounts[(j << 1) + 1];
                    Util.int2bytes(newEntityID, serializedTokenCounts, x);
                    Util.int2bytes(count, serializedTokenCounts, x+4);
                    x += 8;
                    j++;
                } else {
                    System.arraycopy(tokenCountBytes, i << 3, serializedTokenCounts, x, 8);
                    x += 8;
                    i++;
                }
            }

            while (i < numStoredCounts) {
                System.arraycopy(tokenCountBytes, i << 3, serializedTokenCounts, x, 8);
                x += 8;
                i++;
            }

            while (j < numNewCounts) {
                Util.int2bytes(newTokenCounts[j << 1], serializedTokenCounts, x);
                Util.int2bytes(newTokenCounts[(j << 1) + 1], serializedTokenCounts, x + 4);
                x += 8;
                j++;
            }

            serializedTokenCountsSize = x;

        } else {
            serializedTokenCounts = new byte[tokenCounts.size() << 3];
            serializedTokenCountsSize = serializedTokenCounts.length;

            int i=0;
            for (Int2IntMap.Entry entry : tokenCounts.int2IntEntrySet()) {
                Util.int2bytes(entry.getIntKey(), serializedTokenCounts, i);
                Util.int2bytes(entry.getIntValue(), serializedTokenCounts, i+4);
                i+=8;
            }
        }

        byte[] kidBytes;
        ByteArrayOutputStream kidsOut;

        if (edges == null) {
            kidBytes = childrenBytes;
            kidsOut = null;
        } else {
            kidBytes = null;
            kidsOut = new ByteArrayOutputStream();

            byte[] buf = new byte[4];

            Util.int2bytes(edges.size(), buf, 0);

            kidsOut.write(buf);

            for (Int2ObjectMap.Entry<AccumulativeLazyAPT> entry : edges.int2ObjectEntrySet()) {
                Util.int2bytes(entry.getIntKey(), buf, 0);
                try {
                    kidsOut.write(buf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                entry.getValue().writeTo(kidsOut);
            }
        }

        Util.int2bytes(serializedTokenCountsSize, header, 0);

        if (kidBytes != null)
            Util.int2bytes(kidBytes.length, header, 4);
        else
            Util.int2bytes(kidsOut.size(), header, 4);

        out.write(header);
        out.write(serializedTokenCounts, 0, serializedTokenCountsSize);

        if (kidBytes != null)
            out.write(kidBytes);
        else
            kidsOut.writeTo(out);
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
