package uk.ac.susx.tag.apt;



import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;

import java.io.*;
import java.util.Arrays;

/**
 * @author ds300
 */
public class ArrayAPT implements APT {
    private static final int[] EMPTY_INTS = new int[0];
    private static final ArrayAPT[] EMPTY_KIDS = new ArrayAPT[0];

    private int[] edges  = EMPTY_INTS;
    private ArrayAPT[] kids = EMPTY_KIDS;
    private int[] entities = EMPTY_INTS;
    private int[] counts = EMPTY_INTS;
    private int sum = 0;

    private ArrayAPT() {}

    public static final APTFactory<ArrayAPT> factory = new Factory();

    public static class Factory implements APTFactory<ArrayAPT> {
        @Override
        public ArrayAPT read(InputStream inputStream) throws IOException {
            return ArrayAPT.fromStream(inputStream);
        }

        @Override
        public ArrayAPT empty() {
            return new ArrayAPT();
        }

        @Override
        public ArrayAPT[] fromGraph(RGraph graph) {
            final ArrayAPT[] result = new ArrayAPT[graph.entityIds.length];

            final int numRelations = graph.numRelations;
            RGraph.Relation[] relations = graph.relations;
            for (int i=0; i<numRelations; i++) {
                RGraph.Relation r = relations[i];

                if (r.governor >= 0) {
                    ArrayAPT parent = result[r.governor];
                    ArrayAPT child;

                    if (parent == null) {
                        parent = new ArrayAPT();
                        result[r.governor] = parent;
                    }

                    child = parent.getChild(r.type);

                    if (child == null) {
                        child = result[r.dependent];

                        if (child == null) {
                            child = new ArrayAPT();
                            result[r.dependent] = child;
                        }

                        parent.insertChild(r.type, child);
                    }


                    child.insertChild(-r.type, parent);

                    child.insertTokenCount(graph.entityIds[r.dependent], 1);

                    result[r.dependent] = child;

                } else {
                    if (result[r.dependent] == null)
                        result[r.dependent] = new ArrayAPT();
                    result[r.dependent].insertTokenCount(graph.entityIds[r.dependent], 1);
                }
            }

            return result;
        }
    }

    private ArrayAPT copyFor(int dep, ArrayAPT parent, int depth) {
        ArrayAPT result = new ArrayAPT();
        result.entities = entities;
        result.counts = counts;
        result.sum = sum;

        if (depth > 0) {
            result.edges = edges;
            ArrayAPT[] newKids = Arrays.copyOf(kids, kids.length);

            for (int i=0; i<newKids.length; i++) {
                if (edges[i] == dep) {
                    newKids[i] = parent;
                } else {
                    newKids[i] = newKids[i].copyFor(-edges[i], result, depth-1);
                }
            }

            result.kids = newKids;
        }



        return result;
    }


    public ArrayAPT getChildAt(int... path) {
        return getChild(path, 0);
    }

    @Override
    public ArrayAPT getChild(int edge) {
        int idx = Arrays.binarySearch(edges, edge);
        if (idx >= 0)
            return kids[idx];
        else
            return null;
    }

    private ArrayAPT getChild(int[] path, int offset) {
        int idx = Arrays.binarySearch(edges, path[offset]);
        if (idx < 0) {
            return null;
        } else if (offset == path.length - 1) {
            return kids[idx];
        } else {
            return kids[idx].getChild(path, offset+1);
        }
    }



    private void insertChild (int dep, ArrayAPT child) {
        int idx = Arrays.binarySearch(edges, dep);
        if (idx >= 0) {
            kids[idx] = child;
        } else {
            int[] newEdges = new int[edges.length + 1];
            ArrayAPT[] newKids = new ArrayAPT[edges.length + 1];

            int insertionPoint = -(idx + 1);
            if (insertionPoint != 0) {
                System.arraycopy(edges, 0, newEdges, 0, insertionPoint);
                System.arraycopy(kids, 0, newKids, 0, insertionPoint);
            }
            newEdges[insertionPoint] = dep;
            newKids[insertionPoint] = child;
            if (insertionPoint < edges.length) {
                System.arraycopy(edges, insertionPoint, newEdges, insertionPoint + 1, edges.length - insertionPoint);
                System.arraycopy(kids, insertionPoint, newKids, insertionPoint + 1, edges.length - insertionPoint);
            }
            edges = newEdges;
            kids = newKids;
        }
    }

    private void insertTokenCount(int tokenIdx, int count) {
        int idx = Arrays.binarySearch(entities, tokenIdx);
        sum += count;
        if (idx >= 0) {
            counts[idx] += count;
        } else {
            int[] newTokens = new int[entities.length + 1];
            int[] newCounts = new int[entities.length + 1];

            int insertionPoint = -(idx + 1);
            if (insertionPoint != 0) {
                System.arraycopy(entities, 0, newTokens, 0, insertionPoint);
                System.arraycopy(counts, 0, newCounts, 0, insertionPoint);
            }
            newTokens[insertionPoint] = tokenIdx;
            newCounts[insertionPoint] = count;
            if (insertionPoint < entities.length) {
                System.arraycopy(entities, insertionPoint, newTokens, insertionPoint + 1, entities.length - insertionPoint);
                System.arraycopy(counts, insertionPoint, newCounts, insertionPoint + 1, entities.length - insertionPoint);
            }
            entities = newTokens;
            counts = newCounts;
        }
    }

    private static final String space = "|   ";
    private static String repeatString(String s, int n) {
        String result = "";
        for (int i=0; i < n; i++) result += s;
        return result;
    }

    private void printTo(Writer writer, int depth, Resolver<?> tokenResolver, Resolver<?> dependencyResolver, int returnPath) throws IOException {
        writer.write(repeatString(space, depth) + "LABELS\n");

        for (int i=0; i < entities.length; i++) {
            int tkn = entities[i];
            long count = counts[i];
            writer.write(repeatString(space, depth) + "| " + tokenResolver.resolve(tkn).toString() + " ("+count+")\n");
        }

        writer.write(repeatString(space, depth) + "EDGES\n");

        for (int i=0; i < edges.length; i++) {
            int dep = edges[i];
            if (dep != returnPath) {
                ArrayAPT child = kids[i];
                final String relString;
                if (dep < 0) {
                    relString = "_" + dependencyResolver.resolve(-dep).toString();
                } else {
                    relString = dependencyResolver.resolve(dep).toString();
                }
                writer.write(repeatString(space, depth) + "| " + relString + "\n");
                child.printTo(writer, depth + 1, tokenResolver, dependencyResolver, -dep);
            }
        }
    }

    private static final Resolver<Integer> INTEGER_RESOLVER = index -> index;

    public void print() {
        print(INTEGER_RESOLVER, INTEGER_RESOLVER);
    }

    public void print(Resolver<?> tokenResolver, Resolver<?> dependencyResolver) {
        try {
            PrintWriter writer = new PrintWriter(System.out);
            printTo(writer, 0, tokenResolver, dependencyResolver, 0);
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public int getCount(int entityId) {
        int idx = Arrays.binarySearch(entities, entityId);
        if (idx >= 0)
            return counts[idx];
        else
            return 0;
    }

    @Override
    public int sum() {
        return sum;
    }

    @Override
    public ArrayAPT merged(APT other, int depth) {
        if (!(other instanceof ArrayAPT)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                other.writeTo(out);
                other = fromStream(new ByteArrayInputStream(out.toByteArray()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return merge(this, (ArrayAPT) other, depth);
    }

    @Override
    public ArrayAPT culled(int depth) {
        return copyFor(0, this, depth);
    }

    @Override
    public Int2IntSortedMap entityCounts() {
        return new Int2IntArraySortedMap(entities, counts);
    }

    @Override
    public Int2ObjectSortedMap<APT> edges() {
        return new Int2ObjectArraySortedMap<>(edges, kids);
    }

    @Override
    public void walk(APTVisitor visitor) {
        walk(visitor, new int[0], 0);
    }

    private int[] append(int[] a, int x) {
        int[] res = new int[a.length+1];
        System.arraycopy(a,0,res,0,a.length);
        res[a.length] = x;
        return res;
    }

    private void walk(APTVisitor visitor, int[] path, int returnEdge) {
        visitor.visit(path, this);
        for (int i=0;i<edges.length;i++) {
            int edge = edges[i];
            if (edge != returnEdge) {
                kids[i].walk(visitor, append(path, edge), -edge);
            }
        }
    }

    private interface CloneModifier {
        void modify (ArrayAPT clone);
    }

    private ArrayAPT modifyCloned (CloneModifier modifier) {
        ArrayAPT clone = copyFor(0, this, Integer.MAX_VALUE);
        modifier.modify(clone);
        return clone;
    }

    private ArrayAPT ensureArrayAPT (APT thing) {
        if (thing instanceof ArrayAPT) {
            return (ArrayAPT) thing;
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                thing.writeTo(out);
                return fromStream(new ByteArrayInputStream(out.toByteArray()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public ArrayAPT withCount(int entityId, int count) {
        return modifyCloned(clone -> {
            clone.sum += count;
            int idx = Arrays.binarySearch(entities, entityId);
            if (idx < 0) {
                int[] newEntities = new int[entities.length + 1];
                int[] newCounts = new int[entities.length + 1];

                int insertionPoint = -(idx + 1);
                if (insertionPoint != 0) {
                    System.arraycopy(entities, 0, newEntities, 0, insertionPoint);
                    System.arraycopy(counts, 0, newCounts, 0, insertionPoint);
                }
                newEntities[insertionPoint] = entityId;
                newCounts[insertionPoint] = count;
                if (insertionPoint < entities.length) {
                    System.arraycopy(entities, insertionPoint, newEntities, insertionPoint + 1, entities.length - insertionPoint);
                    System.arraycopy(counts, insertionPoint, newCounts, insertionPoint + 1, entities.length - insertionPoint);
                }
                clone.entities = newEntities;
                clone.counts = newCounts;
            } else {
                clone.counts = new int[counts.length];
                System.arraycopy(counts, 0, clone.counts, 0, counts.length);
                clone.counts[idx] += count;
            }
        });
    }

    @Override
    public ArrayAPT withEdge(int rel, APT child) {
        if (rel == 0) throw new IllegalArgumentException("relation ids must not be zero");

        ArrayAPT kid = ensureArrayAPT(child);

        ArrayAPT aligned = kid.getChild(-rel);

        if (aligned == null) {
            aligned = new ArrayAPT();
            ArrayAPT cloned = kid.modifyCloned(clone -> clone.insertChild(-rel, new ArrayAPT()));
            aligned.insertChild(rel, cloned);
        }

        return merge(this, aligned, Integer.MAX_VALUE);
    }


    public static class Labels {
        public int[] tokens;
        public int[] counts;

        public Labels(int[] tokens, int[] counts) {
            this.tokens = tokens;
            this.counts = counts;
        }
    }

    public static ArrayAPT merge(ArrayAPT a, ArrayAPT b, int depth) {
        return merge(a, b, depth, 0, null);
    }

    private static ArrayAPT merge(ArrayAPT a, ArrayAPT b, int depth, int returnPath, ArrayAPT parent) {
        final int[] a_edges = a.edges;
        final int[] b_edges = b.edges;
        final int[] a_tokens = a.entities;
        final int[] b_tokens = b.entities;
        final int[] a_counts = a.counts;
        final int[] b_counts = b.counts;
        final ArrayAPT[] a_kids = a.kids;
        final ArrayAPT[] b_kids = b.kids;


        ArrayAPT result = new ArrayAPT();

        result.sum = a.sum + b.sum;

        final int uniqueLabels = Util.countUnique(a_tokens, b_tokens);
        int[] tokens = new int[uniqueLabels];
        int[] counts = new int[uniqueLabels];

        int i=0, x=0, y=0;

        while (x < a_tokens.length && y < b_tokens.length) {
            if (a_tokens[x] == b_tokens[y]) {
                tokens[i] = a_tokens[x];
                counts[i] = a_counts[x] + b_counts[y];
                x++;
                y++;
            } else if (a_tokens[x] < b_tokens[y]) {
                tokens[i] = a_tokens[x];
                counts[i] = a_counts[x];
                x++;
            } else {
                tokens[i] = b_tokens[y];
                counts[i] = b_counts[y];
                y++;
            }
            i++;
        }

        if (x < a_tokens.length) {
            System.arraycopy(a_tokens, x, tokens, i, a_tokens.length - x);
            System.arraycopy(a_counts, x, counts, i, a_tokens.length - x);
        } else if (y < b_tokens.length) {
            System.arraycopy(b_tokens, y, tokens, i, b_tokens.length - y);
            System.arraycopy(b_counts, y, counts, i, b_tokens.length - y);
        }

        result.entities = tokens;
        result.counts = counts;


        final int uniqueEdges = Util.countUnique(a_edges, b_edges);
        int[] edges = new int[uniqueEdges];
        ArrayAPT[] kids = new ArrayAPT[uniqueEdges];

        i=0; x=0; y=0;

        while (x < a_edges.length && y < b_edges.length) {
            int adep = a_edges[x];
            int bdep = b_edges[y];
            if (adep == bdep) {
                edges[i] = adep;
                if (adep == returnPath) {
                    kids[i] = parent;
                } else {
                    kids[i] = merge(a_kids[x], b_kids[y], depth-1, -adep, result);
                }
                x++;
                y++;
            } else if (adep < bdep) {
                edges[i] = adep;
                kids[i] = a_kids[x].copyFor(-adep, result, depth-1);
                x++;
            } else {
                edges[i] = bdep;
                kids[i] = b_kids[y].copyFor(-bdep, result, depth-1);
                y++;
            }
            i++;
        }

        if (x < a_edges.length) {
            System.arraycopy(a_edges, x, edges, i, a_edges.length - x);
            System.arraycopy(a_kids, x, kids, i, a_edges.length - x);
            while (i < uniqueEdges) {
                kids[i] = kids[i].copyFor(-edges[i], result, depth-1);
                i++;
            }
        } else if (y < b_edges.length) {
            System.arraycopy(b_edges, y, edges, i, b_edges.length - y);
            System.arraycopy(b_kids, y, kids, i, b_edges.length - y);
            while (i < uniqueEdges) {
                kids[i] = kids[i].copyFor(-edges[i], result, depth-1);
                i++;
            }
        }

        result.edges = edges;
        result.kids = kids;

        return result;
    }


    public void writeTo(OutputStream out) throws IOException {
        write(out, 0);
    }

    private void write(OutputStream out, int returnPath) throws IOException {
        byte[] header = new byte[8];
        // write out the number of bytes for storing token counts;
        Util.int2bytes(entities.length << 3, header, 0);

        // write out kids
        byte[] buf = new byte[4];
        ByteArrayOutputStream kidsOut = new ByteArrayOutputStream();

        // number of kids
        if (returnPath == 0) {
            Util.int2bytes(edges.length, buf, 0);
        } else {
            Util.int2bytes(edges.length - 1, buf, 0);
        }
        kidsOut.write(buf);

        for (int i=0; i<kids.length; i++) {
            int edge = edges[i];
            if (edge != returnPath) {
                Util.int2bytes(edge, buf, 0);
                kidsOut.write(buf);
                kids[i].write(kidsOut, -edge);
            }
        }

        int kidBytesLength = kidsOut.size();

        Util.int2bytes(kidBytesLength, header, 4);

        out.write(header);

        byte[] tokenCounts = new byte[entities.length << 3];

        for (int i=0;i< entities.length; i++) {
            Util.int2bytes(entities[i], tokenCounts, i << 3);
            Util.int2bytes(counts[i], tokenCounts, (i << 3) + 4);
        }

        out.write(tokenCounts);

        kidsOut.writeTo(out);
    }

    private static void is4(int i) throws IOException {
        if (i != 4) throw new IOException("Unexpected end of stream");
    }

    public static ArrayAPT fromStream(InputStream in) throws IOException {
        return fromStream(in, 0, null);
    }

    private static ArrayAPT fromStream(InputStream in, int returnPath, ArrayAPT parent) throws IOException {
        final ArrayAPT result = new ArrayAPT();

        final byte[] header = new byte[8];

        if (in.read(header) != 8) throw new IOException("Unable to read APT header");

        final int tokensLength = Util.bytes2int(header, 0);
        final int kidsLength = Util.bytes2int(header, 4);


        if (tokensLength % 8 != 0) throw new IOException("corrupt APT header");

        final int numTokens = tokensLength >>> 3;

        byte[] tokenByte = new byte[tokensLength];

        if (in.read(tokenByte) != tokensLength) throw new IOException("insufficient bytes");

        result.entities = new int[numTokens];
        result.counts = new int[numTokens];

        for (int i=0; i<tokensLength; i+=8) {
            result.entities[i>>>3] = Util.bytes2int(tokenByte, i);
            int count = Util.bytes2int(tokenByte, i+4);
            result.counts[i>>>3] = count;
            result.sum += count;
        }

        final byte[] intBuf = new byte[4];

        if (in.read(intBuf) != 4) throw new IOException("unexpected end of input");

        final int numKids = Util.bytes2int(intBuf);

        if (returnPath == 0) {
            result.edges = new int[numKids];
            result.kids = new ArrayAPT[numKids];
            for (int i=0; i<numKids; i++) {
                if (in.read(intBuf) != 4) throw new IOException("unexpected end of input");
                int edge = Util.bytes2int(intBuf);
                result.edges[i] = edge;
                result.kids[i] = ArrayAPT.fromStream(in, -edge, result);
            }
        } else {
            boolean doneParent = false;
            result.edges = new int[numKids+1];
            result.kids = new ArrayAPT[numKids+1];
            for (int i=0; i<numKids+1; i++) {
                if (i == numKids && !doneParent) {
                    result.edges[i] = returnPath;
                    result.kids[i] = parent;
                } else {
                    if (in.read(intBuf) != 4) throw new IOException("unexpected end of input");
                    int edge = Util.bytes2int(intBuf);
                    if (!doneParent && edge > returnPath) {
                        result.edges[i] = returnPath;
                        result.kids[i] = parent;
                        i++;
                        doneParent = true;
                    }
                    result.edges[i] = edge;
                    result.kids[i] = ArrayAPT.fromStream(in, -edge, result);
                }
            }
        }

        return result;
    }

    private boolean equals(ArrayAPT other, int returnPath) {
        if (other == this) {
            return true;
        } else if (other.sum != sum) {
            return false;
        } else if (other.entities.length != entities.length) {
            return false;
        } else if (other.edges.length != edges.length) {
            return false;
        } else {

            for (int i=0; i<entities.length;i++)
                if (entities[i] != other.entities[i] || counts[i] != other.counts[i])
                    return false;

            for (int i=0; i<kids.length; i++)
                if (edges[i] != other.edges[i] || (edges[i] != returnPath && !kids[i].equals(other.kids[i], -edges[i])))
                    return false;

            return true;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof APT)) {
            return false;
        } else {
            return this.equals(ensureArrayAPT((APT)obj), 0);
        }
    }

}

