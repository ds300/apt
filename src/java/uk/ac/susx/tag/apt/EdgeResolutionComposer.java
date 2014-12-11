package uk.ac.susx.tag.apt;

import java.io.IOException;

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

    @Override
    public ArrayAPT[] compose(PersistentKVStore<Integer, APT> lexicon, RGraph graph) throws IOException {
        int[] sortedIndices = graph.sorted();
        if (direction == Direction.BOTTOM_UP) reverseIntArray(sortedIndices);
        ArrayAPT[] result = new ArrayAPT[sortedIndices.length];
        for (int i=0; i<graph.entityIds.length; i++) {
            result[i] = ArrayAPT.ensureArrayAPT(lexicon.get(graph.entityIds[i]));
        }
        for (int index : sortedIndices) {
            int entityId = graph.entityIds[index];

            for (RGraph.Relation r : graph.relations) {
                if (r.governor < 0) continue;
                if (direction == Direction.BOTTOM_UP && graph.entityIds[r.dependent] == entityId) {
                    result[r.governor] = resolver.resolve(result[r.governor], result[r.dependent], r.type);
                    result[r.dependent] = result[r.governor].getChild(r.type);
                } else if (direction == Direction.TOP_DOWN && graph.entityIds[r.governor] == entityId) {
                    result[r.dependent] = resolver.resolve(result[r.dependent], result[r.governor], -r.type);
                    result[r.governor] = result[r.dependent].getChild(-r.type);
                }
                if (result[r.governor] == null) result[r.governor] = ArrayAPT.factory.empty();
                if (result[r.dependent] == null) result[r.dependent] = ArrayAPT.factory.empty();
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
