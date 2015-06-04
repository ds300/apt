package uk.ac.susx.tag.apt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ds300
 */
public class EdgeResolutionComposer implements APTComposer<ArrayAPT> {

    public static enum Direction {
        TOP_DOWN, BOTTOM_UP
    }

    public static interface NodeIntersectionCalculator {
        Int2FloatArraySortedMap intersect (Int2FloatArraySortedMap a, Int2FloatArraySortedMap b);
    }

    public static interface EdgeResolver {
        ArrayAPT resolve(ArrayAPT accumulator, ArrayAPT resolutionTarget, int resolutionEdge);
    }

    private final EdgeResolver resolver;
    private final Direction direction;

    public EdgeResolutionComposer(EdgeResolver resolver, Direction direction) {
        this.resolver = resolver;
        this.direction = direction;
    }

    public EdgeResolutionComposer(EdgeResolver resolver) {
        this(resolver, Direction.BOTTOM_UP);
    }

    private static void reverseIntArray(final int[] a) {
        for (int i=0, j=a.length-1; i < j; i++, j--) {
            int tmp = a[i];
            a[i] = a[j];
            a[j] = tmp;
        }
    }

    private static int[] reversedPath(int[] b) {
        int[] a = Arrays.copyOf(b, b.length);
        int i,j;
        for (i=0, j=a.length-1; i < j; i++, j--) {
            int tmp = a[i];
            a[i] = -a[j];
            a[j] = -tmp;
        }
        if (i == j) a[i] = -a[i];
        return a;
    }


    @Override
    public ArrayAPT[] compose(PersistentKVStore<Integer, APT> lexicon, final RGraph graph) throws IOException {
        int[] sortedIndices = graph.sorted();
        final ArrayAPT[] determinised = ArrayAPT.factory.fromGraph(graph);

        final int[][] pathFromRoot = new int[graph.entityIds.length][];

        determinised[sortedIndices[0]].walk(new APTVisitor() {
            @Override
            public void visit(int[] path, APT apt) {
                for (int i=0; i<determinised.length; i++) {
                    if (determinised[i] == apt) {
                        pathFromRoot[i] = path;
                    }
                }
            }
        });

        ArrayList<RGraph.Relation>[] outgoingEdges = new ArrayList[graph.entityIds.length];
        ArrayList<RGraph.Relation>[] incomingEdges = new ArrayList[graph.entityIds.length];

        for (RGraph.Relation r : graph.relations) {
            if (r == null) break;
            int gov = r.governor;
            int dep = r.dependent;
            // ignore root dependency
            if (gov >= 0) {
                if (incomingEdges[dep] == null) {
                    incomingEdges[dep] = new ArrayList<>(5);
                }
                incomingEdges[dep].add(r);
                if (outgoingEdges[gov] == null) {
                    outgoingEdges[gov] = new ArrayList<>(5);
                }
                outgoingEdges[gov].add(r);
            }
        }

        ArrayAPT[] result = new ArrayAPT[graph.entityIds.length];
        for (int i=0; i<graph.entityIds.length; i++) {
            result[i] = ArrayAPT.ensureArrayAPT(lexicon.get(graph.entityIds[i]));
        }

        if (direction == Direction.TOP_DOWN) {
            for (int i=0;i<sortedIndices.length;i++) {
                int gov = sortedIndices[i];
                ArrayAPT from = result[gov];
                ArrayList<RGraph.Relation> outgoing = outgoingEdges[gov];
                if (outgoing != null) {
                    for (RGraph.Relation r : outgoing) {
                        int dep = r.dependent;
                        ArrayAPT to = result[dep];
                        from = resolver.resolve(from, to, r.type);
                        to = from.getChild(r.type);
                        if (to == null) {
                            from = from.withEdge(r.type, ArrayAPT.factory.empty());
                            to = from.getChild(r.type);
                        }
                        result[dep] = to;
                    }
                    result[gov] = from;
                    ArrayAPT root = from.getChildAt(reversedPath(pathFromRoot[gov]));
                    result[sortedIndices[0]] = root;
                    for (int j=i-1; j > 0; j--) {
                        int idx = sortedIndices[j];
                        result[idx] = root.getChildAt(pathFromRoot[idx]);
                    }
                }
            }
        }

        return result;
    }



    private static float[] multiply(float[] arr, float alpha) {
        float[] result = arr.clone();
        for (int i=0; i<result.length;i++) result[i] *= alpha;
        return result;
    }

    public static class NodeAlignmentEdgeResolver implements EdgeResolver {

        public final NodeIntersectionCalculator calculator;

        public NodeAlignmentEdgeResolver(NodeIntersectionCalculator calculator) {
            this.calculator = calculator;
        }

        @Override
        public ArrayAPT resolve(ArrayAPT accumulator, ArrayAPT resolutionTarget, int resolutionEdge) {
            // breadth-first alignment
            ArrayAPT a = accumulator.getChild(resolutionEdge);
            if (a == null) {
                a = accumulator.withEdge(resolutionEdge, ArrayAPT.factory.empty()).getChild(resolutionEdge);
            }
            ArrayAPT b = resolutionTarget;

            if (a != null && b != null) {
                ArrayAPT merged = ArrayAPT.merge2(a, b, Integer.MAX_VALUE, new ArrayAPT.ScoreMerger2() {
                    @Override
                    public Int2FloatArraySortedMap merge(ArrayAPT aptA, ArrayAPT aptB, int[] path) {
                        return calculator.intersect(aptA.entityScores(), aptB.entityScores());
                    }
                }, ArrayAPT.EdgeMergePolicy.MERGE_WITH_EMPTY);

                return merged.getChild(-resolutionEdge);
            }
            return accumulator;
        }

    }
}
