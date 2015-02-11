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

    private int[] reversedPath(int[] b) {
        int[] a = Arrays.copyOf(b, b.length);
        for (int i=0, j=a.length-1; i < j; i++, j--) {
            int tmp = a[i];
            a[i] = -a[j];
            a[j] = -tmp;
        }
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


        if (direction == Direction.BOTTOM_UP) reverseIntArray(sortedIndices);
        ArrayAPT[] result = new ArrayAPT[graph.entityIds.length];
        for (int i=0; i<graph.entityIds.length; i++) {
            result[i] = ArrayAPT.ensureArrayAPT(lexicon.get(graph.entityIds[i]));
        }

        for (int i=0; i<sortedIndices.length; i++) {
            int index = sortedIndices[i];
            // todo: put relaltions in more sensible order to avoid wasted iterations here
            for (RGraph.Relation r : graph.relations) {
                if (r == null) break;
                if (r.governor < 0) continue;
                ArrayAPT root;
                if (direction == Direction.BOTTOM_UP && r.dependent == index) {
                    result[r.governor] = resolver.resolve(result[r.governor], result[r.dependent], r.type);
                    root = result[r.governor].getChildAt(reversedPath(pathFromRoot[index]));
                    if (root == null) root = ArrayAPT.factory.empty().withEdge(pathFromRoot[index], result[r.governor]);
                } else if (direction == Direction.TOP_DOWN && r.governor == index) {
                    result[r.dependent] = resolver.resolve(result[r.dependent], result[r.governor], -r.type);
                    root = result[r.dependent].getChildAt(reversedPath(pathFromRoot[index]));
                    if (root == null) root = ArrayAPT.factory.empty().withEdge(pathFromRoot[index], result[r.dependent]);
                } else {
                    continue;
                }
                for (int j=i-1; j >=0; j--) {
                    int idx = sortedIndices[j];
                    result[idx] = root.getChildAt(pathFromRoot[idx]);
                    if (result[idx] == null) {
                        result[idx] = ArrayAPT.factory.empty().withEdge(reversedPath(pathFromRoot[idx]), root);
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
            ArrayAPT b = resolutionTarget;

            if (a != null && b != null) {
                return ArrayAPT.merge2(a, b, Integer.MAX_VALUE, new ArrayAPT.ScoreMerger2() {
                    @Override
                    public Int2FloatArraySortedMap merge(ArrayAPT aptA, ArrayAPT aptB, int[] path) {
                        return calculator.intersect(aptA.entityScores(), aptB.entityScores());
                    }
                }, ArrayAPT.EdgeMergePolicy.MERGE_WITH_EMPTY);
            }
            return accumulator;
        }

    }
}
