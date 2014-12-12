package uk.ac.susx.tag.apt;



import it.unimi.dsi.fastutil.ints.*;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import uk.ac.susx.mlcl.lib.collect.SparseDoubleVector;

/**
 * @author ds300
 */
public class ArrayAPT implements APT {
    private static final int[] EMPTY_INTS = new int[0];
    private static final float[] EMPTY_FLOATS = new float[0];
    private static final ArrayAPT[] EMPTY_KIDS = new ArrayAPT[0];

    int[] edges  = EMPTY_INTS;
    ArrayAPT[] kids = EMPTY_KIDS;
    int[] entities = EMPTY_INTS;
    float[] scores = EMPTY_FLOATS;
    private float sum = 0;

    private ArrayAPT() {}

    public static final APTFactory<ArrayAPT> factory = new Factory();

    public static class Factory implements APTFactory<ArrayAPT> {
        @Override
        public ArrayAPT fromByteArray(byte[] bytes) throws IOException {
            return ArrayAPT.fromByteArray(bytes);
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

            // first iterate over relations and make sure that the tree structure is fully reified
            for (int i=0; i<numRelations; i++) {
                RGraph.Relation r = relations[i];

                // if governor < 0 it is taken to point to the root node
                if (r.governor >= 0) {
                    // this relation is between two real nodes, so find/make them
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
                        }

                        parent.insertChild(r.type, child);
                    }

                    // now add inverse relation from child to parent

                    child.insertChild(-r.type, parent);

                    result[r.dependent] = child;

                } else {
                    // relation governer is root, so just make a new node for the dependent if need be
                    if (result[r.dependent] == null)
                        result[r.dependent] = new ArrayAPT();
                }
            }

            // now iterate over entity ids and, where appropriate, increment the count at their nodes
            for (int i = 0; i < graph.entityIds.length; i++) {
                int eid = graph.entityIds[i];
                ArrayAPT node = result[i];
                if (node != null) {
                    if (eid != -1) {
                        node.incrementScore(eid, 1);
                    } else {
                        // -1 is a pseudo entityID which means "This should contribute towards the probability mass, but
                        // don't bother actually storing the count."
                        node.sum += 1;
                    }
                }
            }

            return result;
        }
    }

    private ArrayAPT copyFor(int dep, ArrayAPT parent, int depth) {
        ArrayAPT result = new ArrayAPT();
        result.entities = entities;
        result.scores = scores;
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
        if (path.length == 0) return this;
        else return getChild(path, 0);
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

    // mutating method, for use during construction time only
    private void incrementScore(int entityId, float score) {
        sum += score;
        int idx = Arrays.binarySearch(entities, entityId);
        if (idx >= 0) {
            scores[idx] += score;
        } else {
            int[] newEntites = new int[entities.length + 1];
            float[] newScores = new float[entities.length + 1];

            int insertionPoint = -(idx + 1);
            if (insertionPoint != 0) {
                System.arraycopy(entities, 0, newEntites, 0, insertionPoint);
                System.arraycopy(scores, 0, newScores, 0, insertionPoint);
            }
            newEntites[insertionPoint] = entityId;
            newScores[insertionPoint] = score;

            if (insertionPoint < entities.length) {
                System.arraycopy(entities, insertionPoint, newEntites, insertionPoint + 1, entities.length - insertionPoint);
                System.arraycopy(scores, insertionPoint, newScores, insertionPoint + 1, entities.length - insertionPoint);
            }

            entities = newEntites;
            scores = newScores;
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
            float score = scores[i];
            writer.write(repeatString(space, depth) + "| " + tokenResolver.resolve(tkn).toString() + " ("+score+")\n");
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

    private static final Resolver<Integer> INTEGER_RESOLVER = new Resolver<Integer>() {
        @Override
        public Integer resolve(int index) {
            return index;
        }

        @Override
        public Iterable<Integer> getIndices() {
            return null;
        }
    };

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
    public float getScore(int entityId) {
        int idx = Arrays.binarySearch(entities, entityId);
        if (idx >= 0)
            return scores[idx];
        else
            return 0;
    }

    @Override
    public float sum() {
        return sum;
    }

    @Override
    public ArrayAPT merged(APT other, int depth) {
        return merge(this, ensureArrayAPT(other), depth);
    }

    @Override
    public ArrayAPT culled(int depth) {
        return copyFor(0, this, depth);
    }

    @Override
    public Int2FloatArraySortedMap entityScores() {
        return new Int2FloatArraySortedMap(entities, scores);
    }

    @Override
    public Int2ObjectSortedMap<APT> edges() {
        return new Int2ObjectArraySortedMap<APT>(edges, kids);
    }

    @Override
    public void walk(APTVisitor visitor) {
        walk(visitor, new int[0], 0);
    }

    private static int[] append(int[] a, int x) {
        int[] res = new int[a.length+1];
        System.arraycopy(a,0,res,0,a.length);
        res[a.length] = x;
        return res;
    }

    private void walk(APTVisitor<ArrayAPT> visitor, int[] path, int returnEdge) {
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

    public static ArrayAPT ensureArrayAPT (APT thing) {
        if (thing instanceof ArrayAPT) {
            return (ArrayAPT) thing;
        } else {
            return fromByteArray(thing.toByteArray());
        }
    }

    @Override
    public ArrayAPT withScore(final int entityId, final float score) {
        return modifyCloned(new CloneModifier() {
            @Override
            public void modify(ArrayAPT clone) {
                int idx = Arrays.binarySearch(entities, entityId);
                if (idx < 0) {
                    clone.sum += score;
                    int[] newEntities = new int[entities.length + 1];
                    float[] newScores = new float[entities.length + 1];

                    int insertionPoint = -(idx + 1);
                    if (insertionPoint != 0) {
                        System.arraycopy(entities, 0, newEntities, 0, insertionPoint);
                        System.arraycopy(scores, 0, newScores, 0, insertionPoint);
                    }
                    newEntities[insertionPoint] = entityId;
                    newScores[insertionPoint] = score;
                    if (insertionPoint < entities.length) {
                        System.arraycopy(entities, insertionPoint, newEntities, insertionPoint + 1, entities.length - insertionPoint);
                        System.arraycopy(scores, insertionPoint, newScores, insertionPoint + 1, entities.length - insertionPoint);
                    }
                    clone.entities = newEntities;
                    clone.scores = newScores;
                } else {
                    clone.sum -= scores[idx];
                    clone.sum += score;
                    clone.scores = new float[scores.length];
                    System.arraycopy(scores, 0, clone.scores, 0, scores.length);
                    clone.scores[idx] = score;
                }
            }
        });
    }

    @Override
    public ArrayAPT withIncrementedScore(final int entityId, final float score) {
        return modifyCloned(new CloneModifier() {
            @Override
            public void modify(ArrayAPT clone) {
                clone.sum += score;
                int idx = Arrays.binarySearch(entities, entityId);
                if (idx < 0) {
                    int[] newEntities = new int[entities.length + 1];
                    float[] newScores = new float[entities.length + 1];

                    int insertionPoint = -(idx + 1);
                    if (insertionPoint != 0) {
                        System.arraycopy(entities, 0, newEntities, 0, insertionPoint);
                        System.arraycopy(scores, 0, newScores, 0, insertionPoint);
                    }
                    newEntities[insertionPoint] = entityId;
                    newScores[insertionPoint] = score;
                    if (insertionPoint < entities.length) {
                        System.arraycopy(entities, insertionPoint, newEntities, insertionPoint + 1, entities.length - insertionPoint);
                        System.arraycopy(scores, insertionPoint, newScores, insertionPoint + 1, entities.length - insertionPoint);
                    }
                    clone.entities = newEntities;
                    clone.scores = newScores;
                } else {
                    clone.scores = Arrays.copyOf(scores, scores.length);
                    clone.scores[idx] += score;
                }
            }
        });
    }


    @Override
    public ArrayAPT withEdge(final int rel, APT child) {
        if (rel == 0) throw new IllegalArgumentException("relation ids must not be zero");

        ArrayAPT kid = ensureArrayAPT(child);

        ArrayAPT aligned = kid.getChild(-rel);

        if (aligned == null) {
            aligned = new ArrayAPT();
            ArrayAPT cloned = kid.modifyCloned(new CloneModifier() {
                @Override
                public void modify(ArrayAPT clone) {
                    clone.insertChild(-rel, new ArrayAPT());
                }
            });
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

    /**
     * instances of ScoreMerger are passed to {@link ArrayAPT#merge} to determine how shared entity scores are combined
     * to form the merged APT.
     */
    public static interface ScoreMerger {
        float merge (float scoreA, float scoreB, int entityId, ArrayAPT a, ArrayAPT b);
        public static final ScoreMerger ADD = new ScoreMerger() {
            @Override
            public float merge(float scoreA, float scoreB, int entityId, ArrayAPT a, ArrayAPT b) {
                return scoreA + scoreB;
            }
        };

        public static final ScoreMerger MULTIPLY = new ScoreMerger() {
            @Override
            public float merge(float scoreA, float scoreB, int entityId, ArrayAPT a, ArrayAPT b) {
                return scoreA * scoreB;
            }
        };

        public static final ScoreMerger MIN = new ScoreMerger() {
            @Override
            public float merge(float scoreA, float scoreB, int entityId, ArrayAPT a, ArrayAPT b) {
                return Math.min(scoreA, scoreB);
            }
        };

        public static final ScoreMerger MAX = new ScoreMerger() {
            @Override
            public float merge(float scoreA, float scoreB, int entityId, ArrayAPT a, ArrayAPT b) {
                return Math.max(scoreA, scoreB);
            }
        };
    }

    public static ArrayAPT merge(ArrayAPT a, ArrayAPT b, int depth) {
        return merge(a, b, depth, ScoreMerger.ADD, 0, null);
    }

    public static ArrayAPT merge(ArrayAPT a, ArrayAPT b, int depth, ScoreMerger scoreMerger) {
        return merge(a, b, depth, scoreMerger, 0, null);
    }

    private static ArrayAPT merge(ArrayAPT a, ArrayAPT b, int depth, ScoreMerger scoreMerger, int returnPath, ArrayAPT parent) {
        final int[] a_edges = a.edges;
        final int[] b_edges = b.edges;
        final int[] a_entities = a.entities;
        final int[] b_entities = b.entities;
        final float[] a_scores = a.scores;
        final float[] b_scores = b.scores;
        final ArrayAPT[] a_kids = a.kids;
        final ArrayAPT[] b_kids = b.kids;


        ArrayAPT result = new ArrayAPT();


        final int uniqueLabels = Util.countUnique(a_entities, b_entities);
        int[] tokens = new int[uniqueLabels];
        float[] scores = new float[uniqueLabels];

        int i=0, x=0, y=0;

        while (x < a_entities.length && y < b_entities.length) {
            if (a_entities[x] == b_entities[y]) {
                tokens[i] = a_entities[x];
                scores[i] = scoreMerger.merge(a_scores[x], b_scores[y], a_entities[x], a, b);
                x++;
                y++;
            } else if (a_entities[x] < b_entities[y]) {
                tokens[i] = a_entities[x];
                scores[i] = a_scores[x];
                x++;
            } else {
                tokens[i] = b_entities[y];
                scores[i] = b_scores[y];
                y++;
            }
            i++;
        }

        if (x < a_entities.length) {
            System.arraycopy(a_entities, x, tokens, i, a_entities.length - x);
            System.arraycopy(a_scores, x, scores, i, a_entities.length - x);
        } else if (y < b_entities.length) {
            System.arraycopy(b_entities, y, tokens, i, b_entities.length - y);
            System.arraycopy(b_scores, y, scores, i, b_entities.length - y);
        }

        result.entities = tokens;
        result.scores = scores;

        result.sum = 0;
        for (float score : result.scores) result.sum += score;


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
                    kids[i] = merge(a_kids[x], b_kids[y], depth-1, scoreMerger, -adep, result);
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

    public static interface ScoreMerger2 {
        Int2FloatArraySortedMap merge(ArrayAPT aptA, ArrayAPT aptB, int[] path);
    }
    public static enum EdgeMergePolicy {
        KEEP_UNMATCHED,
        KEEP_LEFT_UNMATCHED,
        KEEP_RIGHT_UNMATCHED,
        MERGE_WITH_EMPTY,
        DO_NOT_MERGE
    }

    public static interface FloatCombiner {
        float combine (float a, float b);
        float leftOnly(float a);
        float rightOnly(float b);
    }
    public enum ArithmeticOperator implements FloatCombiner {
        ADD {
            @Override
            public float combine (float a, float b) {
                return a + b;
            }

            @Override
            public float leftOnly(float a) {
                return a;
            }

            @Override
            public float rightOnly(float b) {
                return b;
            }
        },
        MULT {
            @Override
            public float combine (float a, float b) {
                return a * b;
            }

            @Override
            public float leftOnly(float a) {
                return 0;
            }

            @Override
            public float rightOnly(float b) {
                return 0;
            }
        },
        // min and max asssume positive numbers.
        MIN {
            @Override
            public float combine (float a, float b) {
                return Math.min(a, b);
            }

            @Override
            public float leftOnly(float a) {
                return 0;
            }

            @Override
            public float rightOnly(float b) {
                return 0;
            }

        },
        MAX {
            @Override
            public float combine (float a, float b) {
                return Math.max(a, b);
            }

            @Override
            public float leftOnly(float a) {
                return a;
            }

            @Override
            public float rightOnly(float b) {
                return b;
            }
        }
    }

    public static class ArithmeticMerger implements ScoreMerger2 {
        private final FloatCombiner combiner;

        public ArithmeticMerger(FloatCombiner combiner) {
            this.combiner = combiner;
        }

        @Override
        public Int2FloatArraySortedMap merge(ArrayAPT aptA, ArrayAPT aptB, int[] path) {
            final int[] a_entities = aptA.entities;
            final int[] b_entities = aptB.entities;
            final float[] a_scores = aptA.scores;
            final float[] b_scores = aptB.scores;

            final int uniqueLabels = Util.countUnique(a_entities, b_entities);
            int[] tokens = new int[uniqueLabels];
            float[] scores = new float[uniqueLabels];

            int i=0, x=0, y=0;

            while (x < a_entities.length && y < b_entities.length) {
                if (a_entities[x] == b_entities[y]) {
                    tokens[i] = a_entities[x];
                    scores[i] = combiner.combine(a_scores[x], b_scores[y]);
                    x++;
                    y++;
                } else if (a_entities[x] < b_entities[y]) {
                    tokens[i] = a_entities[x];
                    scores[i] = a_scores[x];
                    x++;
                } else {
                    tokens[i] = b_entities[y];
                    scores[i] = b_scores[y];
                    y++;
                }
                i++;
            }

            if (x < a_entities.length) {
                System.arraycopy(a_entities, x, tokens, i, a_entities.length - x);
                System.arraycopy(a_scores, x, scores, i, a_entities.length - x);
            } else if (y < b_entities.length) {
                System.arraycopy(b_entities, y, tokens, i, b_entities.length - y);
                System.arraycopy(b_scores, y, scores, i, b_entities.length - y);
            }

            return new Int2FloatArraySortedMap(tokens, scores);
        }
    }

    public static ArrayAPT merge2(ArrayAPT a, ArrayAPT b, int depth, ScoreMerger2 scoreMerger, EdgeMergePolicy emp) {
        return merge2(a, b, depth, scoreMerger, emp, new int[]{}, null);
    }

    private static ArrayAPT merge2(ArrayAPT a, ArrayAPT b, int depth, ScoreMerger2 scoreMerger, EdgeMergePolicy emp, int[] path, ArrayAPT parent) {
        int returnPath = path.length > 0 ? -path[path.length-1] : 0;
        final int[] a_edges = a.edges;
        final int[] b_edges = b.edges;
        final ArrayAPT[] a_kids = a.kids;
        final ArrayAPT[] b_kids = b.kids;


        ArrayAPT result = new ArrayAPT();

        result.sum = a.sum + b.sum;

        Int2FloatArraySortedMap arraySortedMap = scoreMerger.merge(a, b, path);

        result.entities = arraySortedMap.keys;
        result.scores = arraySortedMap.vals;

        result.sum = 0;
        for (float score : result.scores) result.sum += score;

        int[] edges = new int[a_edges.length + b_edges.length];
        ArrayAPT[] kids = new ArrayAPT[edges.length];

        int i=0, // iterate over new arrays, will store their length at the end
            x=0, // iterate over a_edges/kids
            y=0; // iterate over b_edges/kids

        while (x < a_edges.length && y < b_edges.length) {
            int adep = a_edges[x];
            int bdep = b_edges[y];
            if (adep == bdep) {
                edges[i] = adep;
                if (adep == returnPath) {
                    kids[i] = parent;
                } else {
                    kids[i] = merge2(a_kids[x], b_kids[y], depth - 1, scoreMerger, emp, append(path, adep), result);
                }
                x++;
                y++;
                i++;
            } else if (adep < bdep) {
                if (adep == returnPath) {
                    edges[i] = adep;
                    kids[i] = parent;
                    i++;
                } else {
                    switch (emp) {
                        case KEEP_UNMATCHED:
                        case KEEP_LEFT_UNMATCHED:
                            edges[i] = adep;
                            kids[i] = a_kids[x].copyFor(-adep, result, depth-1);
                            i++;
                            break;
                        case MERGE_WITH_EMPTY:
                            edges[i] = adep;
                            kids[i] = merge2(a_kids[x], new ArrayAPT(), depth - 1, scoreMerger, emp, append(path, adep), result);
                            i++;
                            break;
                    }
                }
                x++;
            } else {
                if (bdep == returnPath) {
                    edges[i] = bdep;
                    kids[i] = parent;
                    i++;
                } else {
                    switch (emp) {
                        case KEEP_UNMATCHED:
                        case KEEP_RIGHT_UNMATCHED:
                            edges[i] = bdep;
                            kids[i] = b_kids[y].copyFor(-bdep, result, depth-1);
                            i++;
                            break;
                        case MERGE_WITH_EMPTY:
                            edges[i] = bdep;
                            kids[i] = merge2(new ArrayAPT(), b_kids[y], depth-1, scoreMerger, emp, append(path, bdep), result);
                            i++;
                            break;
                    }
                }
                y++;
            }
        }

        // this isn't very DRY dave
        while (x < a_edges.length) {
            int adep = a_edges[x];
            if (adep == returnPath) {
                edges[i] = adep;
                kids[i] = parent;
                i++;
            } else {
                switch (emp) {
                    case KEEP_UNMATCHED:
                    case KEEP_LEFT_UNMATCHED:
                        edges[i] = adep;
                        kids[i] = a_kids[x].copyFor(-adep, result, depth-1);
                        i++;
                        break;
                    case MERGE_WITH_EMPTY:
                        edges[i] = adep;
                        kids[i] = merge2(a_kids[x], new ArrayAPT(), depth - 1, scoreMerger, emp, append(path, adep), result);
                        i++;
                        break;
                }
            }
            x++;
        }

        while (y < b_edges.length) {
            int bdep = b_edges[y];
            if (bdep == returnPath) {
                edges[i] = bdep;
                kids[i] = parent;
                i++;
            } else {
                switch (emp) {
                    case KEEP_UNMATCHED:
                    case KEEP_RIGHT_UNMATCHED:
                        edges[i] = bdep;
                        kids[i] = b_kids[y].copyFor(-bdep, result, depth-1);
                        i++;
                        break;
                    case MERGE_WITH_EMPTY:
                        edges[i] = bdep;
                        kids[i] = merge2(new ArrayAPT(), b_kids[y], depth-1, scoreMerger, emp, append(path, bdep), result);
                        i++;
                        break;
                }
            }
            y++;
        }

        result.edges = Arrays.copyOf(edges, i);
        result.kids = Arrays.copyOf(kids, i);

        return result;
    }

    public byte[] toByteArray() {
        byte[] bytes = new byte[size(0)];

        if (toByteArray(bytes, 0, 0) != bytes.length) throw new RuntimeException("bad serialization logics");

        return bytes;
    }

    private int toByteArray(final byte[] bytes, final int offset, final int returnPath) {
        int outputOffset = offset + 16;
        for (int i=0; i<entities.length;i++) {
            Util.int2bytes(entities[i], bytes, outputOffset);
            Util.float2bytes(scores[i], bytes, outputOffset+4);
            outputOffset += 8;
        }

        int kidsStart = outputOffset;

        for (int i=0; i<edges.length;i++) {
            int edge = edges[i];
            if (edge != returnPath) {
                Util.int2bytes(edge, bytes, outputOffset);
                outputOffset = kids[i].toByteArray(bytes, outputOffset + 4, -edge);
            }
        }

        int kidsLength = outputOffset - kidsStart;
        int numKids = edges.length + (returnPath == 0 ? 0 : -1);


        // write header
        Util.int2bytes(entities.length << 3, bytes, offset);
        Util.int2bytes(kidsLength, bytes, offset + 4);
        Util.int2bytes(numKids, bytes, offset + 8);
        Util.float2bytes(sum, bytes, offset + 12);

        return outputOffset;
    }

    public static ArrayAPT fromByteArray(byte[] bytes) {
        return fromByteArray(bytes, 0, 0, null).apt;
    }

    private static class OffsetAPTTuple {
        private final int offset;
        private final ArrayAPT apt;

        private OffsetAPTTuple(int offset, ArrayAPT apt) {
            this.offset = offset;
            this.apt = apt;
        }
    }

    private static OffsetAPTTuple fromByteArray(final byte[] bytes, int offset, final int returnPath, final ArrayAPT parent) {
        ArrayAPT result = new ArrayAPT();

        final int numEntitites = Util.bytes2int(bytes, offset) >>> 3;
        final int numKids = Util.bytes2int(bytes, offset + 8) + (parent == null ? 0 : 1);
        result.sum = Util.bytes2float(bytes, offset + 12);

        offset += 16;

        if (numEntitites > 0) {
            int[] entities = new int[numEntitites];
            float[] scores = new float[numEntitites];


            for (int i=0; i < numEntitites; i++) {
                entities[i] = Util.bytes2int(bytes, offset);
                scores[i] = Util.bytes2float(bytes, offset + 4);
                offset += 8;
            }

            result.entities = entities;
            result.scores = scores;
        }

        if (numKids > 0) {
            boolean doneParent = parent == null;

            int[] edges = new int[numKids];
            ArrayAPT[] kids = new ArrayAPT[numKids];
            for (int i=0; i<numKids; i++) {
                if (!doneParent && i == numKids - 1) {
                    edges[i] = returnPath;
                    kids[i] = parent;
                    doneParent = true;
                } else {
                    int edge = Util.bytes2int(bytes, offset);

                    if (!doneParent && returnPath < edge) {
                        edges[i] = returnPath;
                        kids[i] = parent;
                        doneParent = true;
                    } else {
                        edges[i] = edge;
                        OffsetAPTTuple t = fromByteArray(bytes, offset + 4, -edge, result);
                        kids[i] = t.apt;
                        offset = t.offset;
                    }
                }
            }

            result.edges = edges;
            result.kids = kids;
        }

        return new OffsetAPTTuple(offset, result);
    }


    private int size(int returnPath) {
        int s = 16 + (entities.length << 3);

        for (int i=0;i<edges.length;i++) {
            int edge = edges[i];
            if (edge != returnPath) {
                s += 4 + kids[i].size(-edge);
            }
        }

        return s;
    }

    private boolean floatEquals(float a, float b, float tolerance) {
        return a==b || Math.abs(a - b) <= tolerance;
    }

    private boolean equals(ArrayAPT other, int returnPath) {
        if (other == this) {
            return true;
        } else if (!floatEquals(other.sum, sum, 0.0001f)) {
            return false;
        } else if (other.entities.length != entities.length) {
            return false;
        } else if (other.edges.length != edges.length) {
            return false;
        } else {

            for (int i=0; i<entities.length;i++)
                if (entities[i] != other.entities[i] || !floatEquals(scores[i], other.scores[i], 0.0001f))
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


    public Map<int[], Float> extractFeatures () {
        final Map<int[], Float> result = new HashMap<>();
        walk(new APTVisitor<ArrayAPT>() {
            @Override
            public void visit(int[] path, ArrayAPT apt) {
                final int[] es = apt.entities;
                final float[] cs = apt.scores;
                for (int i=0; i < es.length; i++)
                    result.put(append(path, es[i]), cs[i]);
            }
        });
        return result;
    }

    public SparseDoubleVector toSDV (final Indexer<int[]> featureIndexer) {
        final Int2DoubleSortedMap features = new Int2DoubleRBTreeMap();
        walk(new APTVisitor<ArrayAPT>() {
            @Override
            public void visit(int[] path, ArrayAPT apt) {
                final int[] es = apt.entities;
                final float[] cs = apt.scores;
                for (int i=0; i < es.length; i++)
                    features.put(featureIndexer.getIndex(append(path, es[i])), cs[i]);
            }
        });

        int[] keys = new int[features.size()];
        double[] vals = new double[features.size()];

        int i = 0;
        for (Int2DoubleMap.Entry e : features.int2DoubleEntrySet()) {
            keys[i] = e.getIntKey();
            vals[i++] = e.getDoubleValue();
        }

        return new SparseDoubleVector(keys, vals, keys[keys.length-1], keys.length);
    }


}

